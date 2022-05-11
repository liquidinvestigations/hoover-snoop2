"""Definition of Snoop Task System.

This is a simple set of wrappers around Celery functions to afford them stability, reproductability and
result storage. Even while Celery has support for using "result backends" to store the task results, we
didn't enjoy the fact that a power failure or unexpected server restart would wipe out our progress and be
hard to predict. The solution is to mirror all information about running Tasks in a dedicated database, and
use that as the source of thuth.

We also gain something else by mirroring Tasks inside a database table: the ability to de-duplicate running
them, through locking their correspondent rows when running (SQL `SELECT FOR UPDATE`).

Another requirement for this system is the building of Directed Acyclic Graphs (DAGs) of Tasks. The edges of
this graph should carry Task result data from parent task to child task.

As far as alternatives go: Apache Airflow is too slow (takes a few seconds just to run a simple task),
Spotify Luigi does all the scheduling in memory (and can't scale to our needs for persistent
de-duplication), and other K8s-oriented container-native solutions were not investigated. But as a rule of
thumb, if it can't run 1000-5000 idle (no-op) Tasks per minute per CPU, it's too slow for our use.
"""

import cachetools
import random
from contextlib import contextmanager
from io import StringIO
import logging
from time import time, sleep
from datetime import timedelta
from functools import wraps

from django.conf import settings
from django.db import transaction, DatabaseError
from django.utils import timezone

from . import collections
from . import celery
from . import models
from . import indexing
from ..profiler import profile
from .templatetags import pretty_size
from .utils import run_once
from requests.exceptions import ConnectionError
from snoop import tracing

logger = logging.getLogger(__name__)

task_map = {}
ALWAYS_QUEUE_NOW = settings.ALWAYS_QUEUE_NOW


def shuffle_priority(pri):
    """Randomize the task priority: add a randint(-1, 1)
    to argument and return value clamped to [1, 9].

    This helps with spreading out tasks types executed by the different services.
    """

    pri = pri + random.randint(-1, 1)
    pri = max(1, pri)
    pri = min(9, pri)
    return pri


class SnoopTaskError(Exception):
    """Thrown by Task when died and should set status = "error".

    This is be used from inside a Task function to mark unexpected or temporary errors.
    These tasks will be retried after a while until finished.
    """

    def __init__(self, message, details):
        super().__init__(message)
        self.details = details


class SnoopTaskBroken(Exception):
    """Thrown by Task when died and should set status = "broken".

    This is to be used from inside a Task function to mark permanent problems.
    """

    def __init__(self, message, reason):
        super().__init__(message)
        self.reason = reason


class MissingDependency(Exception):
    """Thrown by Task when it depends on another Task that is not finished.
    """

    def __init__(self, name, task):
        self.name = name
        self.task = task


class ExtraDependency(Exception):
    """Thrown by Task when it no longer depends on another Task it used to depend on.

    This happens when a File was not identified correctly and now is;
    different parts of the Task graph must run on it.
    """

    def __init__(self, name):
        self.name = name


def queue_task(task):
    """Queue given Task with Celery to run on a worker.

    If called from inside a transaction, queueing will be done after
    the transaction is finished succesfully.

    Args:
        task: task to be queued in Celery
    """

    def send_to_celery():
        """This does the actual queueing operation.

        This is wrapped in `transactions.on_commit` to avoid
        running it if wrapping transaction fails.
        """
        col = collections.from_object(task)
        try:
            logger.info(f'queueing task {task.func}(pk {task.pk})')
            laterz_snoop_task.apply_async(
                (col.name, task.pk,),
                queue=col.queue_name + '.' + task_map[task.func].queue,
                priority=shuffle_priority(task_map[task.func].priority),
                retry=False,
            )
        except laterz_snoop_task.OperationalError as e:
            logger.error(f'failed to submit {task.func}(pk {task.pk}): {e}')

    import_snoop_tasks()
    if task_map[task.func].bulk:
        return

    queue_length = get_rabbitmq_queue_length(task_map[task.func].queue)
    if queue_length >= settings.DISPATCH_MAX_QUEUE_SIZE:
        return

    transaction.on_commit(send_to_celery)


def queue_next_tasks(task, reset=False):
    """Queues all Tasks that directly depend on this one.

    Args:
        task: will queue running all Tasks in `task.next_set`
        reset: if set, will set next Tasks status to "pending" before queueing it
    """
    with tracing.span('queue_next_tasks'):
        for next_dependency in task.next_set.all():
            next_task = next_dependency.next
            if reset:
                next_task.update(
                    status=models.Task.STATUS_PENDING,
                    error='',
                    broken_reason='',
                    log='',
                    version=task_map[task.func].version,
                )
                next_task.save()

            if task_map[next_task.func].bulk:
                logger.debug("Not queueing bulk task %r after %r", next_task, task)
                continue

            logger.debug("Queueing %r after %r", next_task, task)
            queue_task(next_task)


@run_once
def import_snoop_tasks():
    """Imports task functions from various modules.

    This is required to avoid import loop problems;
    it should be called just before queueing a Task in Celery.
    """
    from . import filesystem  # noqa
    from .analyzers import archives  # noqa
    from .analyzers import text  # noqa
    from .analyzers import email  # noqa
    from .analyzers import emlx  # noqa
    from .analyzers import entities  # noqa
    from .analyzers import exif  # noqa
    from .analyzers import html  # noqa
    from .analyzers import image_classification  # noqa
    from .analyzers import pdf_preview  # noqa
    from .analyzers import pgp  # noqa
    from .analyzers import thumbnails  # noqa
    from .analyzers import tika  # noqa


def is_completed(task):
    """Returns True if Task is in the "success" or "broken" states, and if the task is at the latest
    version.

    Args:
        task: will check `task.status` for values listed above
    """
    COMPLETED = [models.Task.STATUS_SUCCESS, models.Task.STATUS_BROKEN]
    return task.status in COMPLETED and task.version == task_map[task.func].version


@contextmanager
def snoop_task_log_handler(level=logging.DEBUG):
    """Context manager for a text log handler.

    This captures in memory the entire log of running its context.
    It's used to capture Task logs in the database.

    Args:
        level: log level, by default logging.DEBUG
    """
    formatter = logging.Formatter('%(asctime)s %(name)s [%(levelname)s]: %(message)s')
    stream = StringIO()
    handler = logging.StreamHandler(stream)
    handler.setLevel(level)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    # old_root_level = root_logger.level
    # root_logger.setLevel(level)
    root_logger.addHandler(handler)

    try:
        yield handler
    finally:
        handler.flush()
        root_logger.removeHandler(handler)
        # root_logger.setLevel(old_root_level)


@celery.app.task
def laterz_snoop_task(col_name, task_pk, raise_exceptions=False):
    """Celery task used to run snoop Tasks without duplication.

    This function is using Django's `select_for_update` to
    ensure only one instance of a Task is running at one time.
    After running `select_for_update` to lock the row,
    this function will directly call into the main Task switch: `run_task`.

    Args:
        col_name: name of collection where Task is found
        task_pk: primary key of Task
        raise_exceptions: if set, will propagate any Exceptions after Task.status is set to "error"
    """
    import_snoop_tasks()
    col = collections.ALL[col_name]
    with transaction.atomic(using=col.db_alias), col.set_current():
        with snoop_task_log_handler() as handler:
            try:
                task = (
                    models.Task.objects
                    .select_for_update(nowait=True)
                    .get(pk=task_pk)
                )
            except DatabaseError as e:
                logger.error("task %r already running, locked in the database: %s", task_pk, e)
                return
            run_task(task, handler, raise_exceptions)


@profile()
def run_task(task, log_handler, raise_exceptions=False):
    """Runs the main Task switch: get dependencies, run code,
    react to `SnoopTaskError`s, save state and logs, queue next tasks.

    Args:
        task: Task instance to run
        log_handler: instance of log handler to dump results
        raise_exceptions: if set, will propagate any Exceptions after Task.status is set to "error"
    """
    with tracing.trace('run_task'):
        tracing.add_attribute('func', task.func)

        with tracing.span('check task'):
            if is_completed(task):
                logger.debug("%r already completed", task)
                tracing.add_annotation('already completed')
                queue_next_tasks(task)
                return

            args = task.args
            if task.blob_arg:
                assert args[0] == task.blob_arg.pk
                args = [task.blob_arg] + args[1:]

        with tracing.span('check dependencies'):
            depends_on = {}

            all_prev_deps = list(task.prev_set.all())
            if any(dep.prev.status == models.Task.STATUS_ERROR for dep in all_prev_deps):
                logger.info("%r has a dependency in the ERROR state.", task)
                task.update(
                    status=models.Task.STATUS_BROKEN,
                    error='',
                    broken_reason='dependency_has_error',
                    log=log_handler.stream.getvalue(),
                    version=task_map[task.func].version,
                )
                task.save()
                queue_next_tasks(task, reset=True)
                return

            for dep in all_prev_deps:
                prev_task = dep.prev
                if not is_completed(prev_task):
                    task.update(
                        status=models.Task.STATUS_DEFERRED,
                        error='',
                        broken_reason='',
                        log=log_handler.stream.getvalue(),
                        version=task_map[task.func].version,
                    )
                    task.save()
                    logger.info("%r missing dependency %r", task, prev_task)
                    tracing.add_annotation("%r missing dependency %r" % (task, prev_task))
                    queue_task(prev_task)
                    return

                if prev_task.status == models.Task.STATUS_SUCCESS:
                    prev_result = prev_task.result
                elif prev_task.status == models.Task.STATUS_BROKEN:
                    prev_result = SnoopTaskBroken(
                        prev_task.error,
                        prev_task.broken_reason
                    )
                else:
                    raise RuntimeError(f"Unexpected status {prev_task.status}")

                depends_on[dep.name] = prev_result

        with tracing.span('save state before run'):
            task.status = models.Task.STATUS_PENDING
            task.date_started = timezone.now()
            task.date_finished = None
            task.save()

        with tracing.span('run'):
            logger.debug("Running %r", task)
            t0 = time()
            try:
                func = task_map[task.func]
                with tracing.span('call func'):
                    with tracing.trace(name=task.func, service_name='func'):
                        if func.bulk:
                            result = func([task])
                        else:
                            result = func(*args, **depends_on)
                        tracing.add_annotation('success')

                if result is not None:
                    if func.bulk:
                        assert isinstance(result, dict)
                        result_ok = result[task.blob_arg.pk]
                        if not result_ok:
                            raise RuntimeError('bulk task result not OK')
                    else:
                        assert isinstance(result, models.Blob)
                        task.result = result

            except MissingDependency as dep:
                with tracing.span('missing dependency'):
                    msg = 'requests extra dependency: %r, dep = %r [%.03f s]' % (task, dep, time() - t0)
                    logger.info(msg)
                    tracing.add_annotation(msg)

                    task.update(
                        status=models.Task.STATUS_DEFERRED,
                        error='',
                        broken_reason='',
                        log=log_handler.stream.getvalue(),
                        version=task_map[task.func].version,
                    )
                    task.prev_set.get_or_create(
                        prev=dep.task,
                        name=dep.name,
                    )
                    queue_task(task)

            except ExtraDependency as dep:
                with tracing.span('extra dependency'):
                    msg = 'requests to remove dependency: %r, dep = %r [%.03f s]' % (task, dep, time() - t0)
                    logger.info(msg)
                    tracing.add_annotation(msg)

                    task.prev_set.filter(
                        name=dep.name,
                    ).delete()
                    task.update(
                        status=models.Task.STATUS_PENDING,
                        error='',
                        broken_reason='',
                        log=log_handler.stream.getvalue(),
                        version=task_map[task.func].version,
                    )
                    queue_task(task)

            except SnoopTaskBroken as e:
                task.update(
                    status=models.Task.STATUS_BROKEN,
                    error="{}: {}".format(e.reason, e.args[0]),
                    broken_reason=e.reason,
                    log=log_handler.stream.getvalue(),
                    version=task_map[task.func].version,
                )
                msg = 'Broken: %r %s [%.03f s]' % (task, task.broken_reason, time() - t0)
                logger.exception(msg)
                tracing.add_annotation(msg)

            except ConnectionError as e:
                tracing.add_annotation(repr(e))
                logger.exception(e)
                task.update(
                    status=models.Task.STATUS_PENDING,
                    error=repr(e),
                    broken_reason='',
                    log=log_handler.stream.getvalue(),
                    version=task_map[task.func].version,
                )

            except Exception as e:
                if isinstance(e, SnoopTaskError):
                    error = "{} ({})".format(e.args[0], e.details)
                else:
                    error = repr(e)
                logger.exception(e)
                task.update(
                    status=models.Task.STATUS_ERROR,
                    error=error,
                    broken_reason='',
                    log=log_handler.stream.getvalue(),
                    version=task_map[task.func].version,
                )

                msg = 'Failed: %r  %s [%.03f s]' % (task, task.error, time() - t0)
                tracing.add_annotation(msg)
                logger.exception(msg)

                if raise_exceptions:
                    raise
            else:
                logger.debug("Succeeded: %r [%.03f s]", task, time() - t0)
                task.update(
                    status=models.Task.STATUS_SUCCESS,
                    error='',
                    broken_reason='',
                    log=log_handler.stream.getvalue(),
                    version=task_map[task.func].version,
                )

            finally:
                with tracing.span('save state after run'):
                    task.date_finished = timezone.now()
                    task.save()

    if is_completed(task):
        queue_next_tasks(task, reset=True)


def snoop_task(name, priority=5, version=0, bulk=False, queue='default'):
    """Decorator marking a snoop Task function.

    Args:
        name: qualified name of the function, not required to be equal
            to Python module or function name (but recommended)
        priority: int in range [1,9] inclusive, higher is more urgent.
            Passed to celery when queueing.
        version: int, default zero. Statically incremented by programmer when Task code/behavior changes and
            Tasks need to be retried.
        bulk: If set to True, completely deactivates queue_task on this function.
            This task will instead be scheduled periodically in batches. The function decorated with this
            flag will receive a single argument: a list of Task objects containing the individual
            tasks that need to be run.
    """

    def decorator(func):
        def laterz(*args, depends_on={}, retry=False, queue_now=True, delete_extra_deps=False):
            """Actual function doing dependency checking and queueing.

            Args:
                args: positional function arguments
                depends_on: dict with strings mapping to Task instances that this one depends on (and uses
                    their results as keyword arguments) when calling the wrapped function.
                retry: if set, will reset this function even if it's been finished. Otherwise, this doesn't
                    re-trigger a finished function.
                queue_now: If set, will queue this task immediately (the default). Otherwise, tasks will not
                    be left on the queue, and they'll be picked up by the periodic task `run_dispatcher()`
                    in this module.
                delete_extra_deps: If set, will remove any dependencies that are not listed in `depends_on`.
                    Used for fixing dependency graph after its structure or the data evaluation changed.
            """

            if args and isinstance(args[0], models.Blob):
                blob_arg = args[0]
                args = (blob_arg.pk,) + args[1:]

            else:
                blob_arg = None

            task, created = models.Task.objects.get_or_create(
                func=name,
                args=args,
                blob_arg=blob_arg,
            )

            if depends_on:
                for dep_name, dep in depends_on.items():
                    _, created = task.prev_set.get_or_create(
                        prev=dep,
                        name=dep_name,
                    )
                    if created:
                        retry = True

            if delete_extra_deps:
                task.prev_set.exclude(name__in=depends_on.keys()).delete()

            if task.date_finished:
                if retry:
                    retry_task(task)
                return task

            if not bulk:
                if queue_now or ALWAYS_QUEUE_NOW:
                    queue_task(task)
                return task

        def delete(*args):
            """Delete the Task instance with given positional arguments.

            The Task arguments (the dependencies) are not used as primary keys for the Tasks, so they can't
            be used to filter for the Task to delete.

            Args:
                args: the positional arguemts used to fetch the Task.
            """
            if args and isinstance(args[0], models.Blob):
                blob_arg = args[0]
                args = (blob_arg.pk,) + args[1:]

            else:
                blob_arg = None

            task = models.Task.objects.get(
                func=name,
                args=args,
                blob_arg=blob_arg,
            )
            task.delete()

        func.laterz = laterz
        func.delete = delete
        func.priority = priority
        func.version = version
        func.bulk = bulk
        func.queue = queue
        task_map[name] = func
        return func

    return decorator


@cachetools.cached(cache=cachetools.TTLCache(maxsize=100, ttl=20))
def _get_task_funcs_for_queue(queue):
    """Helper function to get all functions for a specific queue.

    Function excludes bulk tasks from listing.

    Value is cached 20s to avoid database load."""

    return [
        x['func']
        for x in models.Task.objects.values('func').distinct()
        if x['func'] in task_map
        and (not task_map[x['func']].bulk
             and task_map[x['func']].queue == queue)
    ]


@cachetools.cached(cache=cachetools.TTLCache(maxsize=100, ttl=20))
def _count_remaining_db_tasks_for_queue_and_status(queue, status):
    """Helper function to count all the tasks in the database for a queue, status combo.

    Function excludes bulk tasks from listing.

    Value is cached 20s to avoid database load."""
    all_funcs = _get_task_funcs_for_queue(queue)
    task_query = models.Task.objects.filter(func__in=all_funcs)
    task_query = task_query.filter(status=status)
    return task_query.count()


def _count_remaining_db_tasks_for_queue(queue):
    """Helper function to count all the tasks in the database for a queue, status combo.

    Function excludes bulk tasks from listing.

    Value is cached 20s to avoid database load."""
    status_list = [
        models.Task.STATUS_PENDING,
        models.Task.STATUS_DEFERRED,
    ]
    count = 0
    for s in status_list:
        count += _count_remaining_db_tasks_for_queue_and_status(queue, s)
    return count


def dispatch_tasks(queue, status=None, outdated=None, newest_first=True):
    """Dispatches (queues) a limited number of Task instances of each type.

    Requires a collection to be selected. Does not dispatch tasks registered with `bulk = True`.

    Queues one batch of `settings.DISPATCH_QUEUE_LIMIT` Tasks for every function type. The function types
    are shuffled before queuing, in an attempt to equalize the processing cycles for different collections
    running at the same time. This is not optional since the message queue has to rearrange these in
    priority order, with only 10 priority levels (and RabbitMQ is very optimized for this task), there isn't
    considerable overhead here.

    Args:
        status: the status used to filter Tasks to dispatch

    Returns:
        bool: True if any tasks have been queued, False if none matching status have been found in current
        collection.
    """

    all_functions = _get_task_funcs_for_queue(queue)
    if status:
        # sort by priority descending, so the queue doesn't have to re-sort elements
        all_functions = sorted(
            all_functions,
            key=lambda x: -task_map[x].priority,
        )
    elif outdated:
        # sort by priority ascending, so we run the dependencies first
        all_functions = sorted(
            all_functions,
            key=lambda x: task_map[x].priority,
        )
    else:
        raise RuntimeError('Must provide arguments: either "status" or "outdated".')

    found_something = False
    for func in all_functions:
        task_query = models.Task.objects.filter(func=func)
        if status:
            task_query = task_query.filter(status=status)
            item_str = status
        if outdated:
            task_query = task_query.exclude(version=task_map[func].version)
            item_str = 'OUTDATED (exclude version {task_map[func].version})'

        if newest_first:
            task_query = task_query.order_by('-date_modified')
        else:
            task_query = task_query.order_by('date_modified')

        task_query = task_query[:settings.DISPATCH_QUEUE_LIMIT]

        task_count = task_query.count()
        if not task_count:
            continue
        logger.info(f'collection "{collections.current().name}": Dispatching {task_count} {item_str} {func} tasks')  # noqa: E501

        for task in task_query.iterator():
            queue_task(task)
        found_something = True
    return found_something


def retry_task(task, fg=False):
    """Resets task status, logs and error messages to their blank value, then re-queues the Task.
    """
    task.update(
        status=models.Task.STATUS_PENDING,
        error='',
        broken_reason='',
        log='',
        version=task_map[task.func].version,
    )
    logger.info("Retrying %r", task)
    task.save()

    if fg:
        col = collections.from_object(task)
        laterz_snoop_task(col.name, task.pk, raise_exceptions=True)
    else:
        queue_task(task)


def retry_tasks(queryset):
    """Efficient re-queueing of an entire QuerySet pointing to Tasks.

    This is using bulk_update to reset the status, logs and error messages on the table; then only queues
    the first `settings.DISPATCH_QUEUE_LIMIT` tasks."""

    logger.info('Retrying %s tasks...', queryset.count())

    all_tasks = queryset.all()
    first_batch = []
    for i in range(0, len(all_tasks), settings.DISPATCH_QUEUE_LIMIT):
        batch = all_tasks[i:i + settings.DISPATCH_QUEUE_LIMIT]
        now = timezone.now()
        fields = ['status', 'error', 'broken_reason', 'log', 'date_modified']

        for task in batch:
            task.status = models.Task.STATUS_PENDING
            task.error = ''
            task.broken_reason = ''
            task.log = ''
            task.date_modified = now
        models.Task.objects.bulk_update(batch, fields, batch_size=2000)

        if not first_batch:
            first_batch = batch[:5000]
            logger.debug('Queueing first %s tasks...', len(first_batch))
            for task in first_batch:
                queue_task(task)

        progress = int(100.0 * (i / len(all_tasks)))
        logger.info('%s%% done' % (progress,))

    logger.info('100% done submitting tasks.')


def require_dependency(name, depends_on, callback, return_error=False):
    """Dynamically adds a dependency to running task.

    Use this when a Task requires the result of another Task, but this is not known when queueing it.

    Args:
        name: name of dependency
        depends_on: current kwargs dict of function. If the name given is missing from this dict, then
            execution will be aborted (by throwing a MissingDependency error), and this Task will have its
            status set to "deferred". When the required task finishes running, this one will be re-queued.
        callback: function that returns Task instance. Will only be called if the dependency was not found.
    """
    if name in depends_on:
        result = depends_on[name]
        if isinstance(result, Exception) and not return_error:
            raise result
        return result

    task = callback()
    raise MissingDependency(name, task)


def remove_dependency(name, depends_on):
    """Dynamically removes a dependency from running task.

    This stops execution, removes the extra dependency in the Task loop and eventually executes this task
    again.
    """
    if name not in depends_on:
        return
    raise ExtraDependency(name)


@snoop_task('do_nothing', queue=None)
def do_nothing(name):
    """No-op task, here for demonstration purposes.

    """
    pass


def returns_json_blob(func):
    """Function decorator that returns a Blob with the JSON-encoded return value of the wrapped function.

    Used in various Task functions to return results in JSON format, while also respecting the fact that
    Task results are always Blobs.


    Warning:
        This function dumps the whole JSON at once, from memory, so this may have problems with very large
        JSON result sizes (>1GB) or dynamically generated results (from a generator).
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        rv = func(*args, **kwargs)
        return models.Blob.create_json(rv)

    return wrapper


def dispatch_walk_tasks():
    """Trigger processing of a collection, starting with its root directory.
    """

    from .filesystem import walk
    root = models.Directory.root()
    assert root, "root document not created"
    walk.laterz(root.pk)


def save_collection_stats():
    """Run the expensive computations to get collection stats, then save result in database.
    """

    from snoop.data.admin import get_stats
    t0 = time()
    get_stats()
    logger.info('stats for collection {} saved in {} seconds'.format(collections.current().name, time() - t0))  # noqa: E501


@cachetools.cached(cache=cachetools.TTLCache(maxsize=100, ttl=20))
def get_rabbitmq_queue_length(q):
    """Fetch queue length from RabbitMQ for a given queue.

    Value is cached; may be at most 20s old.

    Used periodically to decide if we want to queue more functions or not.

    Uses the Management HTTP API of RabbitMQ, since the Celery client doesn not have access to these counts.
    """

    from pyrabbit.api import Client

    try:
        cl = Client(settings.SNOOP_RABBITMQ_HTTP_URL,
                    settings.SNOOP_RABBITMQ_HTTP_USERNAME,
                    settings.SNOOP_RABBITMQ_HTTP_PASSWORD)
        q_depth = cl.get_queue_depth('/', q)
        return q_depth
    except Exception as e:
        logger.warning('cannot get rabbit queue length for queue %s: %s', q, str(e))
        logger.warning('returning length 0 for unknown queue %s', q)
        return 0


def single_task_running(key):
    """Queries both Celery and RabbitMQ to find out if the queue is completely idle.

    Used by all periodic tasks to make sure only one instance is running at any given time. Tasks earlier in
    the queue will exit to make way for the ones that are later in the queue, to make sure the queue will
    never grow unbounded in size if the Task takes more time to run than its execution interval.
    """

    def count_tasks(method, routing_key):
        count = 0
        inspector = celery.app.control.inspect()
        task_list_map = getattr(inspector, method)()
        if task_list_map is None:
            logger.warning('no workers present!')
            return 0

        for tasks in task_list_map.values():
            for task in tasks:
                task_key = task['delivery_info']['routing_key']
                if task_key != routing_key:
                    continue

                count += 1
                logger.info(f'counting {method} task: {str(task)}')

        return count

    logger.info('querying rabbitmq for key %s', key)
    if get_rabbitmq_queue_length(key) > 0:
        return False

    return 1 >= count_tasks('active', routing_key=key) and \
        0 == count_tasks('scheduled', routing_key=key) and \
        0 == count_tasks('reserved', routing_key=key)


@celery.app.task
def save_stats():
    """Periodic Celery task used to save stats for all collections.
    """

    if not single_task_running('save_stats'):
        logger.warning('save_stats function already running, exiting')
        return

    for collection in collections.ALL.values():
        with collection.set_current():
            try:
                save_collection_stats()
            except Exception as e:
                logger.exception(e)
                continue

    # Kill a little bit of time, in case there are a lot of older
    # messages queued up, they have time to fail in the above
    # check.
    sleep(3)


@celery.app.task
def run_dispatcher():
    """Periodic Celery task used to queue next batches of Tasks for each collection.

    We limit the total size of each queue on the message queue, since too many messages on the queue at the
    same time creates performance issues (because the message queue will need to use Disk instead of storing
    everything in memory, thus becoming very slow).
    """

    import_snoop_tasks()
    if not single_task_running('run_dispatcher'):
        logger.warning('run_dispatcher function already running, exiting')
        return

    collection_list = sorted(collections.ALL.values(), key=lambda x: x.name)
    queue_list = list(set(f.queue for f in task_map.values() if f.queue))
    random.shuffle(collection_list)
    random.shuffle(queue_list)
    for collection in collection_list:
        logger.info(f'{"=" * 10} collection "{collection.name}" {"=" * 10}')
        try:
            for q in queue_list:
                if q:
                    dispatch_for(collection, q)
        except Exception as e:
            logger.exception(e)

    sleep(3)


@celery.app.task
def update_all_tags():
    """Periodic Celery task used to re-index documents with changed Tags.

    This task ensures tag editing conflicts (multiple users editing tags for the same document at the same
    time) are fixed in a short time after indexing.
    """

    # circular import
    from . import digests

    if not single_task_running('update_all_tags'):
        logger.warning('run_all_tags function already running, exiting')
        return

    collection_list = sorted(collections.ALL.values(), key=lambda x: x.name)

    for collection in collection_list:
        with collection.set_current():
            logger.info('collection "%r": updating tags', collection)
            digests.update_all_tags()


def dispatch_for(collection, queue):
    """Queue the next batches of Tasks for a given collection.

    This is used to periodically look for new Tasks that must be executed. This queues: "pending" and
    "deferred" tasks left over from previous batches; then adds some Directories to revisit if the
    collection "sync" configuration is set.  Finally, tasks that finished with a temporary error more than a
    predefined number of days ago are also re-queued with the intent of them succeeding.

    The function tends to exit early if any Tasks were found to be queued, as to make sure the Tasks run in
    their natural order (and we're running dependencies before the tasks that require them). The tasks are
    queued by newest first, to make sure the tasks left over from previous batches are being finished first
    (again, to keep the natural order between batches).
    """
    if not collection.process:
        logger.info(f'dispatch: skipping "{collection}", configured with "process = False"')
        return

    with collection.set_current():
        # count tasks in Rabbit and on DB and check if we want to queue more
        queue_len = get_rabbitmq_queue_length(collection.queue_name + '.' + queue)
        db_tasks_remaining = _count_remaining_db_tasks_for_queue(queue)
        if queue_len > 0:
            skip = False
            if queue_len >= settings.DISPATCH_MIN_QUEUE_SIZE:
                skip = True
            if 0 < queue_len <= settings.DISPATCH_MIN_QUEUE_SIZE:
                # skip if we don't have many tasks left --> we would double queue the ones we have
                if db_tasks_remaining <= settings.DISPATCH_QUEUE_LIMIT:
                    skip = True
            if skip:
                logger.info(f'dispatch: skipping {collection}, has {queue_len} queued tasks on q = {queue}')
                return

        funcs_in_queue = [func for func in task_map if task_map[func].queue == queue]

    logger.info('Dispatching for %r, queue = %s', collection, queue)
    from .ocr import dispatch_ocr_tasks

    with collection.set_current():
        if dispatch_tasks(queue, status=models.Task.STATUS_PENDING):
            # if we have enough tasks to not double queue,
            # queue the other end of the database too
            count_pending = _count_remaining_db_tasks_for_queue_and_status(queue, models.Task.STATUS_PENDING)
            if count_pending > 3 * settings.DISPATCH_QUEUE_LIMIT:
                dispatch_tasks(queue, status=models.Task.STATUS_PENDING, newest_first=False)
            logger.info('%r found PENDING tasks, exiting...', collection)
            return True

        # Re-try deferred tasks if we don't have anything in pending. This is to avoid a deadlock.
        # Try the oldest tasks first, since they are the most probable to have complete deps.
        if dispatch_tasks(queue, status=models.Task.STATUS_DEFERRED, newest_first=False):
            logger.info('%r found DEFERRED tasks, exiting...', collection)
            return True

        if queue == 'filesystem':
            count_before = models.Task.objects.count()
            dispatch_walk_tasks()
            dispatch_ocr_tasks()
            count_after = models.Task.objects.count()
            if count_before != count_after:
                logger.info('%r initial dispatch added new tasks, exiting...', collection)
                return True

        # retry outdated tasks
        if dispatch_tasks(queue, outdated=True):
            logger.info('%r found outdated tasks, exiting...', collection)
            return True

        if collection.sync and queue == 'filesystem':
            logger.info("sync: retrying all walk tasks")
            # retry up oldest non-pending walk tasks that are older than 1 min
            retry_tasks(
                models.Task.objects
                .filter(func__in=['filesystem.walk', 'ocr.walk_source'])
                .filter(date_modified__lt=timezone.now() - timedelta(minutes=1))
                .exclude(status=models.Task.STATUS_PENDING)
                .order_by('date_modified')[:settings.SYNC_RETRY_LIMIT_DIRS]
            )

        # retry errors
        error_date = timezone.now() - timedelta(minutes=settings.TASK_RETRY_AFTER_MINUTES)
        old_error_qs = (
            models.Task.objects
            .filter(func__in=funcs_in_queue)
            .filter(status__in=[models.Task.STATUS_BROKEN, models.Task.STATUS_ERROR])
            .filter(fail_count__lt=settings.TASK_RETRY_FAIL_LIMIT)
            .filter(date_modified__lt=error_date)
            .order_by('date_modified')[:settings.RETRY_LIMIT_TASKS]
        )
        if old_error_qs.exists():
            logger.info(f'{collection} found {old_error_qs.count()} ERROR|BROKEN tasks to retry')
            retry_tasks(old_error_qs)

    logger.info(f'dispatch for collection "{collection.name}" done\n')


def get_bulk_tasks_to_run(reverse=False):
    """Checks current collection if we have bulk tasks run.

    Returns: a tuple (TASKS, SIZES) where:
        - TASKS is a dict, keyed by function name, containing a batch of tasks for that function
        - SIZES contains the total size, in bytes, for each task.
    """

    # Max number of tasks to pull.
    # We estimate extra ES metadata: 1 KB / task
    TASK_SIZE_OVERHEAD = 1000

    # stop looking in database after the first X tasks:
    MAX_BULK_TASK_COUNT = 200000

    # Stop adding Tasks to bulk when current size is larger than this 50 MB
    MAX_BULK_SIZE = 50 * (2 ** 20)

    import_snoop_tasks()

    def all_deps_finished(task):
        for dep in task.prev_set.all():
            if dep.prev.status not in [models.Task.STATUS_SUCCESS, models.Task.STATUS_BROKEN]:
                logger.debug('Task %s skipped because dep %s status is %s',
                             task, dep.prev, dep.prev.status)
                return False
            if dep.prev.version != task_map[dep.prev.func].version:
                logger.debug('Task %s skipped because dep %s version = %s, expected = %s',
                             task, dep.prev, dep.prev.version, task_map[dep.prev.func].version)
                return False
        return True

    all_functions = [
        x['func']
        for x in models.Task.objects.values('func').distinct()
        if x['func'] in task_map and task_map[x['func']].bulk
    ]
    task_list = {}
    task_sizes = {}
    for func in all_functions:
        task_list[func] = []
        task_sizes[func] = {}
        current_size = 0

        task_query = (
            models.Task.objects
            .filter(func=func)
            # don't do anything to successful, up to date tasks
            .exclude(status=models.Task.STATUS_SUCCESS, version=task_map[func].version)
        )

        if reverse:
            task_query = task_query.order_by('-date_modified')
        else:
            task_query = task_query.order_by('date_modified')

        for task in task_query[:MAX_BULK_TASK_COUNT]:
            # filter out any taks with non-completed dependencies
            # we could have done this in the DB query above, but it times out on weak machines
            if all_deps_finished(task):
                logger.debug('%s: Selected task %s', func, task)
                task_list[func].append(task)
                task_size = task.size() + TASK_SIZE_OVERHEAD
                current_size += task_size
                task_sizes[func][task.pk] = task_size
                if current_size > MAX_BULK_SIZE:
                    break
        logger.warning('%s: Selected %s items with total size: %s', func, len(task_list[func]), current_size)

    return task_list, task_sizes


def have_bulk_tasks_to_run(reverse=False):
    task_list, _ = get_bulk_tasks_to_run(reverse=False)
    if not task_list:
        return False
    for lst in task_list.values():
        if len(lst) > 0:
            return True
    return False


def run_single_batch_for_bulk_task(reverse=False):
    """Directly runs a single batch for each bulk task type.

    Requires a collection to be selected. Does not dispatch tasks registered with `bulk = False`.

    Returns:
        int: the number of Tasks completed successfully
    """

    total_completed = 0
    all_task_list, all_task_sizes = get_bulk_tasks_to_run(reverse)
    for func in all_task_list:
        task_list = all_task_list[func]
        task_sizes = all_task_sizes[func]
        current_size = sum(task_sizes.values())

        logger.debug('Running single batch of bulk tasks of type: %s', func)
        t0 = timezone.now()
        if not task_list:
            continue

        # set data on rows before running function
        for task in task_list:
            task.status = models.Task.STATUS_PENDING
            task.date_finished = None
            task.date_started = timezone.now()
            task.date_modified = timezone.now()
            task.log = ''
            task.broken_reason = ''
            task.version = task_map[func].version
            task.fail_count = 0
            task.error = ''
        models.Task.objects.bulk_update(task_list, [
            "status",
            "date_finished",
            "date_started",
            "date_modified",
            "log",
            "broken_reason",
            "version",
            "fail_count",
            "error",
        ])
        logger.info(f"Pre-run save on Task objects took {(timezone.now() - t0).total_seconds():0.2f}s")

        # Run the bulk task. If it failed, mark all the items inside as failed. Otherwise, mark them as
        # succeeded.
        try:
            result = task_map[func](task_list)
        except Exception as e:
            logger.exception(e)
            error = str(e)[:2000]
            status = models.Task.STATUS_ERROR
        else:
            status = models.Task.STATUS_SUCCESS
            error = ''
            logger.info(f"Successfully ran bulk of {len(task_list)} tasks, "
                        f"type {func}, size {pretty_size.pretty_size(current_size)}")

        t_elapsed = (timezone.now() - t0).total_seconds()

        # save results
        for task in task_list:
            task.status = status if result.get(task.blob_arg.pk) else models.Task.STATUS_BROKEN
            task.date_finished = timezone.now()
            # adjust date started so duration is scaled for task size
            current_task_size = task_sizes[task.pk]
            relative_duration = t_elapsed * current_task_size / current_size
            task.date_started = task.date_finished - timedelta(seconds=relative_duration)
            task.date_modified = timezone.now()
            task.error = error

        models.Task.objects.bulk_update(task_list, [
            "status",
            "date_finished",
            "date_started",
            "date_modified",
            "error",
        ])

        if status == models.Task.STATUS_SUCCESS:
            total_completed += len(task_list)

    return total_completed


def _run_bulk_tasks_for_collection():
    """Helper method that runs a number of bulk task batches in the current collection."""

    # Stop processing each collection after this many batches or seconds
    BATCHES_IN_A_ROW = 100
    MAX_FAILED_BATCHES = 10
    SECONDS_IN_A_ROW = 300

    import_snoop_tasks()

    t0 = timezone.now()
    failed_count = 0
    for i in range(int(BATCHES_IN_A_ROW / 2)):
        try:
            with transaction.atomic():
                count = run_single_batch_for_bulk_task(reverse=False)

            with transaction.atomic():
                count += run_single_batch_for_bulk_task(reverse=True)

        except Exception as e:
            failed_count += 1
            if failed_count > MAX_FAILED_BATCHES:
                raise

            logger.error("Failed to run single batch! Attempt #%s", failed_count)
            logger.exception(e)
            sleep(30)
            continue
        if not count:
            break
        if (timezone.now() - t0).total_seconds() > SECONDS_IN_A_ROW:
            logger.warning("Stopping after %s batches because of timeout: %s/%s seconds",
                           i + 1,
                           int((timezone.now() - t0).total_seconds()),
                           SECONDS_IN_A_ROW)
            break


@celery.app.task
def run_bulk_tasks():
    """Periodic task that runs some batches of bulk tasks for all collections.
    For each collection, we update the ES index refresh interval."""

    if not single_task_running('run_bulk_tasks'):
        logger.warning('run_bulk_tasks function already running, exiting')
        return

    all_collections = list(collections.ALL.values())
    random.shuffle(all_collections)
    for collection in all_collections:
        # if no tasks to do, continue
        with collection.set_current():
            if not collection.process:
                logger.info(f'bulk tasks: skipping "{collection}", configured with "process = False"')
                continue

            if not have_bulk_tasks_to_run(reverse=False) and not have_bulk_tasks_to_run(reverse=True):
                logger.info('Skipping collection %s, no bulk tasks to run', collection.name)
                continue

            # disable refreshing
            logger.info('Disable index refresh for collection %s', collection.name)
            indexing.update_refresh_interval("-1")

            try:
                logger.info('Running bulk tasks for collection %s', collection.name)
                _run_bulk_tasks_for_collection()
            except Exception:
                logger.error("Running bulk tasks failed for %s!", collection.name)
            finally:
                # restore default
                logger.info('Enable index refresh for collection %s', collection.name)
                indexing.update_refresh_interval()
