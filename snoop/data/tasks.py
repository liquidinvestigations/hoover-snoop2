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
import traceback
from time import time, sleep
from datetime import timedelta
from functools import wraps
import os
import fcntl

from django.conf import settings
from django.db import transaction, DatabaseError
from django.utils import timezone
import pyrabbit.api

from . import tracing
from . import collections
from . import celery
from . import models
from . import indexing
from .templatetags import pretty_size
from .utils import run_once
from requests.exceptions import ConnectionError

logger = logging.getLogger(__name__)
tracer = tracing.Tracer(__name__)

task_map = {}
ALWAYS_QUEUE_NOW = settings.ALWAYS_QUEUE_NOW
QUEUE_ANOTHER_TASK_LIMIT = 100000
QUEUE_ANOTHER_TASK = 'snoop.data.tasks.queue_another_task'
QUEUE_ANOTHER_TASK_BATCH_COUNT = 1000


def _flock_acquire(lock_path):
    """Acquire lock file at given path.

    Lock is exclusive, errors return immediately instead of waiting."""
    open_mode = os.O_RDWR | os.O_CREAT | os.O_TRUNC
    fd = os.open(lock_path, open_mode)
    try:
        # LOCK_EX = exclusive
        # LOCK_NB = not blocking
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except Exception as e:
        os.close(fd)
        logger.warning('failed to get lock at ' + lock_path + ": " + str(e))
        raise

    return fd


def _flock_release(fd):
    """Release lock file at given path."""
    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


@contextmanager
def _flock_contextmanager(lock_path):
    """Creates context with exclusive file lock at given path."""
    fd = _flock_acquire(lock_path)
    try:
        yield
    finally:
        _flock_release(fd)


def flock(func):
    """Function decorator that makes use of exclusive file lock to ensure
    only one function instance is running at a time.

    All function runners must be present on the same container for this to work."""
    LOCK_FILE_BASE = '/tmp'
    file_name = f'_snoop_flock_{func.__name__}.lock'
    lock_path = os.path.join(LOCK_FILE_BASE, file_name)

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            with _flock_contextmanager(lock_path):
                return func(*args, **kwargs)
        except Exception as e:
            logger.warning('function already running: %s, %s', func.__name__, str(e))
            return
    return wrapper


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


def rmq_queue_name(func, collection=None):
    """Get rabbitmq name from function, collection.
    Collection is inferred by default.
    """
    if collection is None:
        collection = collections.current()
    # return task_map[func].queue + '.' + func
    return collection.queue_name + '.' + task_map[func].queue + '.' + func


@tracer.wrap_function()
def queue_task(task):
    """Queue given Task with Celery to run on a worker.

    If called from inside a transaction, queueing will be done after
    the transaction is finished succesfully.

    Args:
        task: task to be queued in Celery
    """
    import_snoop_tasks()
    if task_map[task.func].bulk:
        return
    col = collections.from_object(task)

    # if queue is full, abort
    queue_length = get_rabbitmq_queue_length(rmq_queue_name(task.func))
    if queue_length >= settings.DISPATCH_MAX_QUEUE_SIZE or _is_rabbitmq_memory_full():
        return

    def send_to_celery():
        """This does the actual queueing operation.

        This is wrapped in `transactions.on_commit` to avoid
        running it if wrapping transaction fails.
        """
        with col.set_current():
            try:
                logger.debug(f'queueing task {task.func}(pk {task.pk})')
                laterz_snoop_task.apply_async(
                    (col.name, task.pk,),
                    queue=rmq_queue_name(task.func),
                    retry=False,
                )
            except laterz_snoop_task.OperationalError as e:
                logger.error(f'failed to submit {task.func}(pk {task.pk}): {e}')

    # Check the lock on the task. If we get it, set status = QUEUED and send to celery"""
    with col.set_current():
        with transaction.atomic(using=col.db_alias), tracer.span('task fetch mark started'):
            try:
                task = (
                    models.Task.objects
                    .select_for_update(nowait=True)
                    .get(pk=task.pk)
                )
                task.update(status=models.Task.STATUS_QUEUED)
                task.save()
                transaction.on_commit(send_to_celery)
            except DatabaseError as e:
                logger.warning(
                    "queue_task(): collection %s: task %r already locked: %s",
                    col.name, task.pk, e,
                )
                tracer.count('queue_task_locked')
                return
            except models.Task.DoesNotExist:
                logger.error(
                    "queue_task(): collection %s: task pk=%s DOES NOT EXIST IN DB",
                    col.name, task.pk
                )
                tracer.count('queue_task_not_found')
                return


def queue_next_tasks(*a, **kw):
    """Queues all Tasks that directly depend on this one.

    Also queues a pending task of the same type.

    Args:
        task: will queue running all Tasks in `task.next_set`
        reset: if set, will set next Tasks status to "pending" before queueing it
    """
    # attempt to avoid locks by running the update outside the first transaction
    transaction.on_commit(lambda: _do_queue_next_tasks(*a, **kw))


@tracer.wrap_function()
def _do_queue_next_tasks(task, reset=False):
    """Implementation for `queue_next_tasks`.
    This is offset to the end of the Tx using on_commit."""
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

    if settings.SNOOP_TASK_DISABLE_TAIL_QUEUE:
        return

    # batch things toghether probabilistically when calling queue_another_task
    if random.random() < 1 / QUEUE_ANOTHER_TASK_BATCH_COUNT:
        if _is_rabbitmq_memory_full():
            return
        if get_rabbitmq_queue_length(QUEUE_ANOTHER_TASK) < QUEUE_ANOTHER_TASK_LIMIT:
            queue_another_task.apply_async(
                (collections.current().name, task.func,),
                queue=QUEUE_ANOTHER_TASK,
                retry=False,
            )


@celery.app.task
@tracer.wrap_function()
def queue_another_task(collection_name, func, *args, **kw):
    """Queue a different task.

    Decoupled from "queue_next_tasks" to remove ourselves from the database transaction
    concerning previous task.
    """

    if _is_rabbitmq_memory_full():
        return

    with collections.ALL[collection_name].set_current():
        db_alias = collections.current().db_alias
        queue_length = get_rabbitmq_queue_length(rmq_queue_name(func))
        if queue_length < settings.DISPATCH_MAX_QUEUE_SIZE - QUEUE_ANOTHER_TASK_BATCH_COUNT:
            with tracer.span('queue another task of same type'), \
                    transaction.atomic(using=db_alias):
                tasks = (
                    models.Task.objects
                    .select_for_update(skip_locked=True)
                    .filter(status=models.Task.STATUS_PENDING, func=func)
                    .order_by('date_modified')[:int(QUEUE_ANOTHER_TASK_BATCH_COUNT)].all()
                )
                for task in tasks:
                    queue_task(task)

            with tracer.span('queue another task of any type'), \
                    transaction.atomic(using=db_alias):
                tasks = (
                    models.Task.objects
                    .select_for_update(skip_locked=True)
                    .filter(status=models.Task.STATUS_PENDING)
                    .order_by('date_modified')[:int(QUEUE_ANOTHER_TASK_BATCH_COUNT)].all()
                )
                for task in tasks:
                    queue_task(task)

        if random.random() < 0.1:
            with tracer.span('queue some errors'), \
                    transaction.atomic(using=db_alias):
                for age_minutes, retry_limit in [
                    (settings.TASK_RETRY_AFTER_MINUTES, settings.TASK_RETRY_FAIL_LIMIT),  # ~5min
                    (settings.TASK_RETRY_AFTER_MINUTES * 30, settings.TASK_RETRY_FAIL_LIMIT * 2),  # ~1h
                    (settings.TASK_RETRY_AFTER_MINUTES * 1000, settings.TASK_RETRY_FAIL_LIMIT * 3),  # ~5day
                ]:
                    old_error_qs = (
                        models.Task.objects
                        .select_for_update(skip_locked=True)
                        .filter(func=func)
                        .filter(status__in=[models.Task.STATUS_BROKEN, models.Task.STATUS_ERROR])
                        .filter(fail_count__lt=retry_limit)
                        .filter(date_modified__lt=timezone.now() - timedelta(minutes=age_minutes))
                        .order_by('date_modified')[:QUEUE_ANOTHER_TASK_BATCH_COUNT].all()
                    )
                    for task in old_error_qs:
                        queue_task(task)
                        return

        if random.random() < 0.1:
            with tracer.span('mark some killed task'), \
                    transaction.atomic(using=db_alias):
                for age_minutes, retry_limit in [
                    (settings.TASK_RETRY_AFTER_MINUTES, settings.TASK_RETRY_FAIL_LIMIT),  # ~5min
                    (settings.TASK_RETRY_AFTER_MINUTES * 30, settings.TASK_RETRY_FAIL_LIMIT * 2),  # ~1h
                    (settings.TASK_RETRY_AFTER_MINUTES * 1000, settings.TASK_RETRY_FAIL_LIMIT * 3),  # ~5day
                ]:
                    # mark dead STARTED tasks as error (hangs / memory leaks / kills)
                    old_started_qs = (
                        models.Task.objects
                        .select_for_update(skip_locked=True)
                        .filter(func=func)
                        .filter(fail_count__lt=retry_limit)
                        .filter(status__in=[models.Task.STATUS_STARTED])
                        .filter(date_modified__lt=timezone.now() - timedelta(minutes=age_minutes))
                        .order_by('date_modified')[:QUEUE_ANOTHER_TASK_BATCH_COUNT].all()
                    )
                    for task in old_started_qs:
                        if not is_task_running(task.pk):
                            logger.debug('marking task %s as Killed', task.pk)
                            tracer.count("task_killed")
                            task.status = models.Task.STATUS_BROKEN
                            task.error = "Task Killed"
                            task.broken_reason = "task_killed"
                            task.fail_count += 1
                            task.save()
                            return

        if random.random() < 0.1:
            with tracer.span('queue some deferred'), \
                    transaction.atomic(using=db_alias):
                DEFERRED_WAIT_MIN = 15
                tasks = (
                    models.Task.objects
                    .select_for_update(skip_locked=True)
                    .filter(func=func)
                    .filter(status=models.Task.STATUS_DEFERRED,
                            date_modified__lt=timezone.now() - timedelta(minutes=DEFERRED_WAIT_MIN))
                    .order_by('date_modified')[:QUEUE_ANOTHER_TASK_BATCH_COUNT].all()
                )
                for task in tasks:
                    queue_task(task)


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


def is_task_running(task_pk):
    """Check if a started task is still running, by trying to get the lock for it."""

    with transaction.atomic(using=collections.current().db_alias):
        try:
            task = (
                models.Task.objects
                .select_for_update(nowait=True)
                .get(pk=task_pk)
            )
            logger.warning('got lock for task %s, task is DEAD', task.pk)
            return False
        except DatabaseError as e:
            logger.debug('task is RUNNING, error while fetching lock: %s', e)
            return True


@celery.app.task
@tracer.wrap_function()
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
    logger.debug('collection %s: starting task %s', col_name, task_pk)

    def lock_children(task):
        """Lock all children tasks to make sure we can update them after the results."""
        next_tasks = [dep.next for dep in task.next_set.all()]
        for next_task in next_tasks:
            try:
                next_locked = (
                    models.Task.objects
                    .select_for_update(nowait=True)
                    .get(pk=next_task.pk)
                )
                logger.debug('locked child: %s', next_locked.pk)
            except Exception as e:
                logger.warning('failed to lock child: %s -> %s (%s)',
                               task.func, next_task.func, str(e))
                raise

    with snoop_task_log_handler() as handler:
        with col.set_current():
            # first tx & select for update: get task, set status STARTED, save, end tx (commit)
            with transaction.atomic(using=col.db_alias), tracer.span('task fetch mark started'):
                try:
                    task = (
                        models.Task.objects
                        .select_for_update(nowait=True)
                        .get(pk=task_pk)
                    )
                    lock_children(task)

                except DatabaseError as e:
                    logger.warning(
                        "collection %s: task %r already running (1st check), locked in db: %s",
                        col_name, task_pk, e,
                    )
                    tracer.count('task_already_running')
                    return
                except models.Task.DoesNotExist:
                    logger.error(
                        "collection %s: task pk=%s DOES NOT EXIST IN DB",
                        col_name, task_pk
                    )
                    tracer.count('task_not_found')
                    return

            _tracer_opt = {
                'attributes': {
                    'function': task.func,
                    'function_group': task.func.split('.')[0] if '.' in task.func else task.func,
                    'collection': collections.current().name,
                },
                'extra_counters': {
                    'size_bytes': {
                        "unit": "b",
                        "value": task.size(),
                    },
                },
            }

            with tracer.span('check if task already completed', **_tracer_opt):
                if is_completed(task):
                    logger.debug("%r already completed", task)
                    tracer.count('task_already_completed', **_tracer_opt)
                    queue_next_tasks(task)
                    return

            with tracer.span('check dependencies', **_tracer_opt):
                depends_on = {}

                all_prev_deps = list(task.prev_set.all())
                if any(dep.prev.status == models.Task.STATUS_ERROR for dep in all_prev_deps):
                    logger.debug("%r has a dependency in the ERROR state.", task)
                    task.update(
                        status=models.Task.STATUS_BROKEN,
                        error='',
                        broken_reason='dependency_has_error',
                        log=handler.stream.getvalue(),
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
                            log=handler.stream.getvalue(),
                            version=task_map[task.func].version,
                        )
                        task.save()
                        logger.debug("%r missing dependency %r", task, prev_task)
                        tracer.count("task_missing_dependency", **_tracer_opt)
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

            with tracer.span('save state before run', **_tracer_opt):
                task.status = models.Task.STATUS_STARTED
                task.date_started = timezone.now()
                task.date_modified = timezone.now()
                task.date_finished = None
                task.save()

            # second tx & select for update: get task, run task
            with transaction.atomic(using=col.db_alias):
                with tracer.span('task fetch lock object'):
                    try:
                        task = (
                            models.Task.objects
                            .select_for_update(nowait=True)
                            .get(pk=task_pk)
                        )
                        lock_children(task)
                    except DatabaseError as e:
                        logger.error(
                            "collection %s: task %r already running (2nd check), locked in db: %s",
                            col_name, task_pk, e,
                        )
                        return
                run_task(task, depends_on, handler, raise_exceptions, _tracer_opt)


def run_task(task, depends_on, log_handler, raise_exceptions=False, _tracer_opt=dict()):
    """Runs the main Task switch: get dependencies, run code,
    react to `SnoopTaskError`s, save state and logs, queue next tasks.

    Args:
        task: Task instance to run
        log_handler: instance of log handler to dump results
        raise_exceptions: if set, will propagate any Exceptions after Task.status is set to "error"
    """
    with tracer.span('run_task', **_tracer_opt):
        tracer.count("task_started", **_tracer_opt)

        args = task.args
        if task.blob_arg:
            assert args[0] == task.blob_arg.pk
            args = [task.blob_arg] + args[1:]

        with tracer.span('run task function', **_tracer_opt):
            logger.debug("Running %r", task)
            t0 = time()
            try:
                func = task_map.get(task.func)
                if not func:
                    msg = "task func " + task.func + ' does not exist.'
                    raise SnoopTaskBroken(msg, 'unknown_task_func')

                with tracer.span('call function', **_tracer_opt):
                    if func.bulk:
                        result = func([task])
                    else:
                        result = func(*args, **depends_on)

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
                with tracer.span('handle missing dependency', **_tracer_opt):
                    tracer.count("task_missing_dependency", **_tracer_opt)
                    msg = 'requests extra dependency: %r, dep = %r [%.03f s]' % (task, dep, time() - t0)
                    logger.debug(msg)

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
                with tracer.span('handle extra dependency', **_tracer_opt):
                    tracer.count("task_extra_dependency", **_tracer_opt)
                    msg = 'requests to remove dependency: %r, dep = %r [%.03f s]' % (task, dep, time() - t0)
                    logger.debug(msg)

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
                with tracer.span('handle task broken', **_tracer_opt):
                    tracer.count("task_broken", **_tracer_opt)
                    task.update(
                        status=models.Task.STATUS_BROKEN,
                        error="{}: {}".format(e.reason, e.args[0]),
                        broken_reason=e.reason,
                        log=log_handler.stream.getvalue(),
                        version=task_map[task.func].version,
                    )
                    msg = 'Broken: %r %s [%.03f s]' % (task, task.broken_reason, time() - t0)
                    logger.exception(msg)

            except ConnectionError as e:
                with tracer.span('handle connection error', **_tracer_opt):
                    tracer.count("task_connection_error", **_tracer_opt)
                    logger.exception(e)
                    task.update(
                        status=models.Task.STATUS_PENDING,
                        error=repr(e),
                        broken_reason='',
                        log=log_handler.stream.getvalue(),
                        version=task_map[task.func].version,
                    )

            except Exception as e:
                with tracer.span('handle unknown error', **_tracer_opt):
                    tracer.count("task_unknown_error", **_tracer_opt)
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
                    logger.exception(msg)

                    if raise_exceptions:
                        raise
            else:
                with tracer.span('save success', **_tracer_opt):
                    tracer.count("task_success", **_tracer_opt)
                    logger.debug("Succeeded: %r [%.03f s]", task, time() - t0)
                    task.update(
                        status=models.Task.STATUS_SUCCESS,
                        error='',
                        broken_reason='',
                        log=log_handler.stream.getvalue(),
                        version=task_map[task.func].version,
                    )

            finally:
                with tracer.span('save state after run', **_tracer_opt):
                    task.date_finished = timezone.now()
                    task.save()

    if is_completed(task):
        queue_next_tasks(task, reset=True)


def snoop_task(name, version=0, bulk=False, queue='default'):
    """Decorator marking a snoop Task function.

    Args:
        name: qualified name of the function, not required to be equal
            to Python module or function name (but recommended)
        version: int, default zero. Statically incremented by programmer when Task code/behavior changes and
            Tasks need to be retried.
        bulk: If set to True, completely deactivates queue_task on this function.
            This task will instead be scheduled periodically in batches. The function decorated with this
            flag will receive a single argument: a list of Task objects containing the individual
            tasks that need to be run.
    """

    def decorator(func):
        # add telemetry to all snoop tasks
        func = tracer.wrap_function()(func)

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

            if task.date_finished \
                    and task.status in [models.Task.STATUS_SUCCESS,
                                        models.Task.STATUS_BROKEN,
                                        models.Task.STATUS_ERROR]:
                if retry:
                    retry_task(task)
                return task

            if not bulk:
                if queue_now or ALWAYS_QUEUE_NOW:
                    queue_task(task)
                return task
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
        func.version = version
        func.bulk = bulk
        func.queue = queue
        func.func = name
        task_map[name] = func
        return func

    return decorator


@cachetools.cached(cache=cachetools.TTLCache(maxsize=500, ttl=settings.TASK_COUNT_MEMORY_CACHE_TTL))
@tracer.wrap_function()
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


@cachetools.cached(cache=cachetools.TTLCache(maxsize=500, ttl=settings.TASK_COUNT_MEMORY_CACHE_TTL))
@tracer.wrap_function()
def _count_remaining_db_tasks_for_queue_and_status(func, status, collection):
    """Helper function to count all the tasks in the database for a queue, status combo.

    Function excludes bulk tasks from listing.

    Value is cached 20s to avoid database load.
    Collection argument is needed to separate caches."""
    return models.Task.objects.filter(func=func, status=status).count()


@tracer.wrap_function()
def _count_remaining_db_tasks_for_queue(func):
    """Helper function to count all the tasks in the database for a queue, status combo.

    Function excludes bulk tasks from listing.

    Value is cached 20s to avoid database load."""
    status_list = [
        models.Task.STATUS_PENDING,
        models.Task.STATUS_DEFERRED,
    ]
    count = 0
    for s in status_list:
        count += _count_remaining_db_tasks_for_queue_and_status(
            func, s, collections.current().name)
    return count


@tracer.wrap_function()
def dispatch_tasks(func, status=None, outdated=None, newest_first=True):
    """Dispatches (queues) a limited number of Task instances of each type.

    Requires a collection to be selected. Does not dispatch tasks registered with `bulk = True`.

    Queues one batch of `settings.DISPATCH_QUEUE_LIMIT` Tasks for every function type. The function types
    are shuffled before queuing, in an attempt to equalize the processing cycles for different collections
    running at the same time.
    Args:
        status: the status used to filter Tasks to dispatch

    Returns:
        bool: True if any tasks have been queued, False if none matching status have been found in current
        collection.
    """

    # with transaction.atomic(using=collections.current().db_alias):
    found_something = False
    task_query = models.Task.objects.filter(
        func=func, date_modified__lt=timezone.now() - timedelta(minutes=1)
    )
    if status:
        task_query = (
            task_query
            # .select_for_update(skip_locked=True)
            .filter(status=status)
        )
        item_str = status
    if outdated:
        task_query = task_query.exclude(version=task_map[func].version)
        item_str = f'OUTDATED (exclude version {task_map[func].version})'

    if newest_first:
        task_query = task_query.order_by('-date_modified')
    else:
        task_query = task_query.order_by('date_modified')

    # if outdated, use retry_tasks to mark everything as pending
    # from the start (to get actual ETA, not 99.99%)
    if outdated:
        found_something = task_query.exists()
        if found_something:
            logger.info(f'collection "{collections.current().name}": Dispatching {item_str} {func} tasks')  # noqa: E501
            retry_tasks(task_query, reset_fail_count=True)

    if found_something:
        return found_something

    task_query = task_query[:settings.DISPATCH_QUEUE_LIMIT]

    task_count = task_query.count()
    if not task_count:
        return found_something
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
    logger.debug("Retrying %r", task)
    task.save()

    if fg:
        col = collections.from_object(task)
        laterz_snoop_task(col.name, task.pk, raise_exceptions=True)
    else:
        queue_task(task)


@tracer.wrap_function()
def retry_tasks(queryset, reset_fail_count=False, one_slice_only=False):
    """Efficient re-queueing of an entire QuerySet pointing to Tasks.

    This is using bulk_update to reset the status, logs and error messages on the table; then only queues
    the first few thousand tasks."""

    # relatively low number to avoid memory leak / crash
    BATCH_SIZE = 1000

    logger.info('Retrying %s tasks...', queryset.count())

    task_count = queryset.count()
    first_batch = list(queryset.all()[0:BATCH_SIZE])

    if one_slice_only:
        fields = ['status', 'error', 'broken_reason', 'log', 'date_modified']
        for task in first_batch:
            task.status = models.Task.STATUS_PENDING
            task.error = ''
            task.broken_reason = ''
            task.log = ''
            task.date_modified = timezone.now()
            if reset_fail_count:
                task.fail_count = 0
        models.Task.objects.bulk_update(first_batch, fields, batch_size=BATCH_SIZE)
    else:
        update_options = {
            'status': models.Task.STATUS_PENDING,
            'error': '',
            'broken_reason': '',
            'log': '',
            'date_modified': timezone.now(),
        }
        if reset_fail_count:
            update_options['fail_count'] = 0
        queryset.update(**update_options)

    logger.info('Queueing first %s tasks...', task_count)
    for task in first_batch:
        queue_task(task)
    logger.info('Done queueing first %s tasks.', task_count)

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


def dispatch_directory_walk_tasks(directory_pk):
    """Trigger processing of a specific directory.

    Returns: A string that is the full path of the directory.
    """

    from .filesystem import walk
    directory = models.Directory.objects.get(pk=directory_pk)
    assert directory, "Directory does not exist"
    walk.laterz(directory.pk)


@tracer.wrap_function()
def save_collection_stats():
    """Run the expensive computations to get collection stats, then save result in database.
    """

    from snoop.data.admin import get_stats
    t0 = time()
    get_stats()
    logger.debug('stats for collection {} saved in {} seconds'.format(collections.current().name, time() - t0))  # noqa: E501


@cachetools.cached(cache=cachetools.TTLCache(maxsize=50, ttl=settings.TASK_COUNT_MEMORY_CACHE_TTL))
@tracer.wrap_function()
def _is_rabbitmq_memory_full():
    """Return True if rabbitmq memory is full (more than 70% of max)."""
    MEMORY_FILL_MAX = 0.70

    try:
        cl = pyrabbit.api.Client(
            settings.SNOOP_RABBITMQ_HTTP_URL,
            settings.SNOOP_RABBITMQ_HTTP_USERNAME,
            settings.SNOOP_RABBITMQ_HTTP_PASSWORD,
        )

        nodes = cl.get_nodes()
        failed = False
        for node in nodes:
            hard_limit = node['mem_limit']
            soft_limit = int(hard_limit * MEMORY_FILL_MAX)
            use = node['mem_used']
            if use >= soft_limit:
                logger.warning('rabbitmq memory full; node = %s, use = %s, soft limit = %s, hard limit = %s',
                               node['name'], use, soft_limit, hard_limit)
                failed = True
        return failed

    except Exception as e:
        logger.error('error when fetching rabbitmq ndoe memory depth: %s', e)
        return False


@tracer.wrap_function()
def get_rabbitmq_queue_length_no_cache(q):
    """Fetch queue length from RabbitMQ for a given queue.

    Used periodically to decide if we want to queue more functions or not.

    Uses the Management HTTP API of RabbitMQ, since the Celery client doesn not have access to these counts.
    """

    def _get_queue_depth(q):
        cl = pyrabbit.api.Client(
            settings.SNOOP_RABBITMQ_HTTP_URL,
            settings.SNOOP_RABBITMQ_HTTP_USERNAME,
            settings.SNOOP_RABBITMQ_HTTP_PASSWORD,
        )
        return cl.get_queue('/', q).get('messages', 0)

    try:
        return _get_queue_depth(q)
    except Exception as e:
        logger.warning('error when fetching queue depth: %s', e)
        return 0


@cachetools.cached(cache=cachetools.TTLCache(maxsize=500, ttl=settings.TASK_COUNT_MEMORY_CACHE_TTL))
def get_rabbitmq_queue_length(q):
    """Fetch queue length from RabbitMQ for a given queue.

    Value is cached; may be at most 20s old.
    """
    return get_rabbitmq_queue_length_no_cache(q)


def single_task_running(key):
    """Queries both Celery and RabbitMQ to find out if the queue is completely idle.

    Used by all periodic tasks to make sure only one instance is running at any given time. Tasks earlier in
    the queue will exit to make way for the ones that are later in the queue, to make sure the queue will
    never grow unbounded in size if the Task takes more time to run than its execution interval.
    """
    return get_rabbitmq_queue_length_no_cache(key) <= 1


@celery.app.task
@tracer.wrap_function()
@flock
def save_stats():
    """Periodic Celery task used to save stats for all collections.
    """

    deadline = time() + settings.SYSTEM_TASK_DEADLINE_SECONDS

    if not single_task_running('save_stats'):
        logger.warning('save_stats function already running, exiting')
        return

    shuffled_col_list = list(collections.ALL.values())
    random.shuffle(shuffled_col_list)

    for collection in shuffled_col_list:
        if time() > deadline:
            break
        with collection.set_current():
            try:
                save_collection_stats()
            except Exception as e:
                logger.exception(e)
                continue


@celery.app.task
@tracer.wrap_function()
@flock
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
    func_list = sorted(set(f.func for f in task_map.values() if f.queue))
    random.shuffle(collection_list)
    random.shuffle(func_list)
    for collection in collection_list:
        logger.info(f'{"=" * 10} collection "{collection.name}" {"=" * 10}')
        try:
            for func in func_list:
                if func:
                    dispatch_for(collection, func)
        except Exception as e:
            logger.exception(e)


@celery.app.task
@tracer.wrap_function()
@flock
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
    random.shuffle(collection_list)

    deadline = time() + settings.SYSTEM_TASK_DEADLINE_SECONDS
    for collection in collection_list:
        with collection.set_current():
            logger.debug('collection "%r": updating tags', collection)
            digests.update_all_tags()
        if time() > deadline:
            break


@tracer.wrap_function()
def dispatch_for(collection, func):
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
        logger.debug(f'dispatch: skipping "{collection}", configured with "process = False"')
        return

    with collection.set_current():
        # count tasks in Rabbit and on DB and check if we want to queue more
        queue_len = get_rabbitmq_queue_length_no_cache(rmq_queue_name(func))
        if queue_len == 0:
            # check if we have any queued tasks in the DB. if we do, they all need to be re-queued...
            db_invalid_queued_tasks = models.Task.objects.filter(func=func, status=models.Task.STATUS_QUEUED)
            if db_invalid_queued_tasks.exists():
                logger.warning(
                    "collection %s func %s: db has %s queued records, but rabbit has %s! resetting db...",
                    collection.name, func,
                    db_invalid_queued_tasks.count(),
                    queue_len,
                )
                db_invalid_queued_tasks.update(status=models.Task.STATUS_PENDING)
        db_tasks_remaining = _count_remaining_db_tasks_for_queue(func)
        if queue_len > 0:
            skip = False
            if queue_len >= settings.DISPATCH_MIN_QUEUE_SIZE or _is_rabbitmq_memory_full():
                skip = True
            if 0 < queue_len <= settings.DISPATCH_MIN_QUEUE_SIZE:
                # skip if we don't have many tasks left --> we would double queue the ones we have
                if db_tasks_remaining <= settings.DISPATCH_QUEUE_LIMIT:
                    skip = True
            if skip:
                logger.info(f'dispatch: skipping {collection}, has {queue_len} queued tasks on f = {func}')
                return

    logger.debug('Dispatching for %r, func = %s', collection, func)
    from .ocr import dispatch_ocr_tasks

    with collection.set_current():
        if dispatch_tasks(func, status=models.Task.STATUS_PENDING):
            # if we have enough tasks to not double queue,
            # queue the other end of the database too
            count_pending = _count_remaining_db_tasks_for_queue_and_status(
                func, models.Task.STATUS_PENDING,
                collections.current().name,
            )
            if count_pending > 3 * settings.DISPATCH_QUEUE_LIMIT:
                dispatch_tasks(func, status=models.Task.STATUS_PENDING, newest_first=False)
            logger.debug('%r found PENDING tasks, exiting...', collection)
            return True

        if func.startswith('filesystem'):
            count_before = models.Task.objects.count()
            dispatch_walk_tasks()
            dispatch_ocr_tasks()
            count_after = models.Task.objects.count()
            if count_before != count_after:
                logger.debug('%r initial dispatch added new tasks, exiting...', collection)
                return True

        # Re-try deferred tasks if we don't have anything in pending. This is to avoid a deadlock.
        # Try the oldest tasks first, since they are the most probable to have complete deps.
        if dispatch_tasks(func, status=models.Task.STATUS_DEFERRED, newest_first=False):
            logger.debug('%r found DEFERRED tasks, exiting...', collection)
            return True

        # retry outdated tasks
        if dispatch_tasks(func, outdated=True):
            logger.debug('%r found outdated tasks, exiting...', collection)
            return True

        if collection.sync and func in ['filesystem.walk', 'ocr.walk_source']:
            logger.debug("sync: retrying all walk tasks")
            # retry up oldest non-pending walk tasks that are older than 3 min

            retry_tasks(
                models.Task.objects
                .filter(func=func)
                .filter(date_modified__lt=timezone.now() - timedelta(minutes=3))
                .filter(status=models.Task.STATUS_SUCCESS)
                .order_by('date_modified')[:settings.SYNC_RETRY_LIMIT_DIRS],
                one_slice_only=True,
            )

        # retry errors
        for age_minutes, retry_limit in [
            (settings.TASK_RETRY_AFTER_MINUTES, settings.TASK_RETRY_FAIL_LIMIT),  # ~5min
            (settings.TASK_RETRY_AFTER_MINUTES * 30, settings.TASK_RETRY_FAIL_LIMIT * 2),  # ~1h
            (settings.TASK_RETRY_AFTER_MINUTES * 1000, settings.TASK_RETRY_FAIL_LIMIT * 3),  # ~5day
        ]:
            old_error_qs = (
                models.Task.objects
                .filter(func=func)
                .filter(status__in=[models.Task.STATUS_BROKEN, models.Task.STATUS_ERROR])
                .filter(fail_count__lt=retry_limit)
                .filter(date_modified__lt=timezone.now() - timedelta(minutes=age_minutes))
                .order_by('date_modified')[:settings.RETRY_LIMIT_TASKS]
            )
            if old_error_qs.exists():
                logger.info(f'{collection} found {old_error_qs.count()} ERROR|BROKEN tasks to retry')
                retry_tasks(old_error_qs, one_slice_only=True)
                return True

            # mark dead STARTED tasks as error (hangs / memory leaks / kills)
            old_started_qs = (
                models.Task.objects
                .filter(func=func)
                .filter(fail_count__lt=retry_limit)
                .filter(status__in=[models.Task.STATUS_STARTED])
                .filter(date_modified__lt=timezone.now() - timedelta(minutes=age_minutes))
                .order_by('date_modified')[:settings.RETRY_LIMIT_TASKS]
            )
            if old_started_qs.exists():
                logger.debug(f'{collection} found {old_started_qs.count()} old STARTED tasks to check')
                for started_task in old_started_qs:
                    if not is_task_running(started_task.pk):
                        logger.debug('marking task %s as Killed', started_task.pk)
                        tracer.count("task_killed")
                        started_task.status = models.Task.STATUS_BROKEN
                        started_task.error = "Task Killed"
                        started_task.broken_reason = "task_killed"
                        started_task.fail_count += 1
                        started_task.save()
                return True

    logger.debug(f'dispatch for collection "{collection.name}" done\n')


@tracer.wrap_function()
def get_bulk_tasks_to_run(reverse=False, exclude_deferred=False, deferred_only=False, lock=True):
    """Checks current collection if we have bulk tasks run.

    Returns: a tuple (TASKS, SIZES, MARKED) where:
        - TASKS is a dict, keyed by function name, containing a batch of tasks for that function
        - SIZES contains the total size, in bytes, for each task.
        - MARKED contains the count of tasks marked here as deferred (instead of being returned)
    """

    # Max number of tasks to pull.
    # We estimate extra ES metadata: 2 KB / task
    TASK_SIZE_OVERHEAD = 2000

    # stop looking in database after the first X tasks:
    MAX_BULK_TASK_COUNT = 300

    # Stop adding Tasks to bulk when current size is larger than this 30 MB
    MAX_BULK_SIZE = 30 * (2 ** 20)

    marked_deferred = 0

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

        task_query = models.Task.objects
        if lock:
            task_query = task_query.select_for_update(skip_locked=True)
        task_query = (
            task_query
            .filter(func=func)
            # don't do anything to successful, up to date tasks
            .exclude(status=models.Task.STATUS_SUCCESS, version=task_map[func].version)
        )
        if exclude_deferred:
            task_query = task_query.exclude(status=models.Task.STATUS_DEFERRED)
        if deferred_only:
            task_query = task_query.filter(status=models.Task.STATUS_DEFERRED)

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
            else:
                # deps not finished ==> set this as DEFERRED
                task.status = models.Task.STATUS_DEFERRED
                task.save()
                marked_deferred += 1
        logger.warning('%s: Selected %s items with total size: %s', func, len(task_list[func]), current_size)

    if marked_deferred > 0:
        logger.debug('marked %s tasks as deferred.', marked_deferred)
    return task_list, task_sizes, marked_deferred


def have_bulk_tasks_to_run(reverse=False):
    task_list, _, marked = get_bulk_tasks_to_run(lock=False)
    if not task_list:
        return False
    for lst in task_list.values():
        if len(lst) > 0:
            return True
    return marked > 0


@tracer.wrap_function()
def run_single_batch_for_bulk_task(reverse=False, exclude_deferred=False, deferred_only=False):
    """Directly runs a single batch for each bulk task type.

    Requires a collection to be selected. Does not dispatch tasks registered with `bulk = False`.

    Returns:
        int: the number of Tasks completed successfully or marked as Deferred
    """

    total_completed = 0
    all_task_list, all_task_sizes, marked = get_bulk_tasks_to_run(reverse, exclude_deferred, deferred_only)
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
        logger.debug(f"Pre-run save on Task objects took {(timezone.now() - t0).total_seconds():0.2f}s")

        # Run the bulk task. If it failed, mark all the items inside as failed. Otherwise, mark them as
        # succeeded.
        try:
            result = task_map[func](task_list)
        except Exception:
            logger.exception(f'Error running bulk task: "{func}"!')
            error = traceback.format_exc()[:2000]
            status = models.Task.STATUS_ERROR
            result = {}
        else:
            status = models.Task.STATUS_SUCCESS
            error = ''
            logger.debug(f"Successfully ran bulk of {len(task_list)} tasks, "
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

    return total_completed + marked


@tracer.wrap_function()
def _run_bulk_tasks_for_collection():
    """Helper method that runs a number of bulk task batches in the current collection."""

    # Stop processing each collection after this many batches or seconds
    BATCHES_IN_A_ROW = 100
    MAX_FAILED_BATCHES = 10
    SECONDS_IN_A_ROW = settings.SYSTEM_TASK_DEADLINE_SECONDS / 2

    import_snoop_tasks()

    t0 = timezone.now()
    failed_count = 0
    for i in range(int(BATCHES_IN_A_ROW / 3)):
        try:
            with transaction.atomic(using=collections.current().db_alias):
                count = run_single_batch_for_bulk_task(reverse=False, exclude_deferred=True)

            with transaction.atomic(using=collections.current().db_alias):
                count += run_single_batch_for_bulk_task(reverse=True, exclude_deferred=True)

            with transaction.atomic(using=collections.current().db_alias):
                count += run_single_batch_for_bulk_task(deferred_only=True)

        except Exception as e:
            failed_count += 1
            if failed_count > MAX_FAILED_BATCHES:
                raise

            logger.error("Failed to run single batch! Attempt #%s", failed_count)
            logger.exception(e)
            sleep(5)
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
@tracer.wrap_function()
@flock
def run_bulk_tasks():
    """Periodic task that runs some batches of bulk tasks for all collections.
    For each collection, we update the ES index refresh interval."""

    if not single_task_running('run_bulk_tasks'):
        logger.warning('run_bulk_tasks function already running, exiting')
        return

    all_collections = list(collections.ALL.values())
    random.shuffle(all_collections)
    deadline = settings.SYSTEM_TASK_DEADLINE_SECONDS + time()
    for collection in all_collections:
        # if no tasks to do, continue
        with collection.set_current():
            if not collection.process:
                logger.debug(f'bulk tasks: skipping "{collection}", configured with "process = False"')
                continue

            if not have_bulk_tasks_to_run(reverse=False) and not have_bulk_tasks_to_run(reverse=True):
                logger.debug('Skipping collection %s, no bulk tasks to run', collection.name)
                continue

            # disable refreshing
            logger.debug('Disable index refresh for collection %s', collection.name)
            indexing.update_refresh_interval("-1")

            try:
                logger.debug('Running bulk tasks for collection %s', collection.name)
                _run_bulk_tasks_for_collection()
            except Exception:
                logger.error("Running bulk tasks failed for %s!", collection.name)
            finally:
                # restore default
                logger.debug('Enable index refresh for collection %s', collection.name)
                indexing.update_refresh_interval()
        if time() > deadline:
            break
