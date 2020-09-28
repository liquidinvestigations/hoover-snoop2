from contextlib import contextmanager
from io import StringIO
import json
import logging
from time import time, sleep
from datetime import timedelta

from celery.bin.control import inspect
from django.conf import settings
from django.db import transaction, DatabaseError
from django.utils import timezone

from . import collections
from . import celery
from . import models
from ..profiler import profile
from .utils import run_once
from requests.exceptions import ConnectionError
from snoop import tracing

logger = logging.getLogger(__name__)

task_map = {}


class SnoopTaskError(Exception):

    def __init__(self, message, details):
        super().__init__(message)
        self.details = details


class SnoopTaskBroken(Exception):

    def __init__(self, message, reason):
        super().__init__(message)
        self.reason = reason


class MissingDependency(Exception):

    def __init__(self, name, task):
        self.name = name
        self.task = task


def queue_task(task):
    import_snoop_tasks()

    def send_to_celery():
        col = collections.from_object(task)
        try:
            laterz_snoop_task.apply_async(
                (col.name, task.pk,),
                queue=col.queue_name,
                priority=task_map[task.func].priority,
                retry=False,
            )
            logger.debug(f'queued task {task.func}(pk {task.pk})')
        except laterz_snoop_task.OperationalError as e:
            logger.error(f'failed to submit {task.func}(pk {task.pk}): {e}')

    transaction.on_commit(send_to_celery)


def queue_next_tasks(task, reset=False):
    with tracing.span('queue_next_tasks'):
        for next_dependency in task.next_set.all():
            next_task = next_dependency.next
            if reset:
                next_task.update(
                    status=models.Task.STATUS_PENDING,
                    error='',
                    broken_reason='',
                    log='',
                )
                next_task.save()
            logger.info("Queueing %r after %r", next_task, task)
            queue_task(next_task)


@run_once
def import_snoop_tasks():
    from . import filesystem  # noqa
    from .analyzers import archives  # noqa
    from .analyzers import text  # noqa


def is_completed(task):
    COMPLETED = [models.Task.STATUS_SUCCESS, models.Task.STATUS_BROKEN]
    return task.status in COMPLETED


@contextmanager
def snoop_task_log_handler(level=logging.DEBUG):
    stream = StringIO()
    handler = logging.StreamHandler(stream)
    handler.setLevel(level)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    try:
        yield handler
    finally:
        root_logger.removeHandler(handler)


@celery.app.task
def laterz_snoop_task(col_name, task_pk, raise_exceptions=False):
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
    with tracing.trace('run_task'):
        tracing.add_attribute('func', task.func)

        with tracing.span('check task'):
            if is_completed(task):
                logger.info("%r already completed", task)
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
                    broken_reason='has a dependency in the ERROR state',
                    log=log_handler.stream.getvalue(),
                )
                task.save()
                return

            for dep in all_prev_deps:
                prev_task = dep.prev
                if not is_completed(prev_task):
                    task.update(
                        status=models.Task.STATUS_DEFERRED,
                        error='',
                        broken_reason='',
                        log=log_handler.stream.getvalue(),
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
            logger.info("Running %r", task)
            t0 = time()
            try:
                func = task_map[task.func]
                with tracing.span('call func'):
                    with tracing.trace(name=task.func, service_name='func'):
                        result = func(*args, **depends_on)
                        tracing.add_annotation('success')

                if result is not None:
                    assert isinstance(result, models.Blob)
                    task.result = result

            except MissingDependency as dep:
                with tracing.span('missing dependency'):
                    msg = '%r requests an extra dependency: %r [%.03f s]' % (task, dep, time() - t0)
                    logger.info(msg)
                    tracing.add_annotation(msg)

                    task.update(
                        status=models.Task.STATUS_DEFERRED,
                        error='',
                        broken_reason='',
                        log=log_handler.stream.getvalue(),
                    )
                    task.prev_set.get_or_create(
                        prev=dep.task,
                        name=dep.name,
                    )
                    queue_task(task)

            except SnoopTaskBroken as e:
                task.update(
                    status=models.Task.STATUS_BROKEN,
                    error="{}: {}".format(e.reason, e.args[0]),
                    broken_reason=e.reason,
                    log=log_handler.stream.getvalue(),
                )
                msg = '%r broken: %s [%.03f s]' % (task, task.broken_reason, time() - t0)
                logger.exception(msg)
                tracing.add_annotation(msg)

            except ConnectionError as e:
                tracing.add_annotation(repr(e))
                logger.exception(repr(e))
                task.update(
                    status=models.Task.STATUS_DEFERRED,
                    error=repr(e),
                    broken_reason='',
                    log=log_handler.stream.getvalue(),
                )

            except Exception as e:
                if isinstance(e, SnoopTaskError):
                    error = "{} ({})".format(e.args[0], e.details)
                else:
                    error = repr(e)
                task.update(
                    status=models.Task.STATUS_ERROR,
                    error=error,
                    broken_reason='',
                    log=log_handler.stream.getvalue(),
                )

                msg = '%r failed: %s [%.03f s]' % (task, task.error, time() - t0)
                tracing.add_annotation(msg)
                logger.exception(msg)

                if raise_exceptions:
                    raise
            else:
                logger.info("%r succeeded [%.03f s]", task, time() - t0)
                task.update(
                    status=models.Task.STATUS_SUCCESS,
                    error='',
                    broken_reason='',
                    log=log_handler.stream.getvalue(),
                )

            finally:
                with tracing.span('save state after run'):
                    task.date_finished = timezone.now()
                    task.save()

    if is_completed(task):
        queue_next_tasks(task, reset=True)


def snoop_task(name, priority=5):

    def decorator(func):

        def laterz(*args, depends_on={}, retry=False, queue_now=True):
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

            if task.date_finished:
                if retry:
                    retry_task(task)
                return task

            if queue_now:
                queue_task(task)
            return task

        func.laterz = laterz
        func.priority = priority
        task_map[name] = func
        return func

    return decorator


def dispatch_tasks(status):
    all_functions = [x['func'] for x in models.Task.objects.values('func').distinct()]
    found_something = False

    for func in all_functions:
        task_query = (
            models.Task.objects
            .filter(status=status, func=func)
            .order_by('-date_modified')  # newest pending tasks first
        )[:settings.DISPATCH_QUEUE_LIMIT]

        task_count = task_query.count()
        if not task_count:
            logger.info(f'collection "{collections.current().name}": No {status} {func} tasks to dispatch')  # noqa: E501
            continue
        logger.info(f'collection "{collections.current().name}": Dispatching {task_count} {status} {func} tasks')  # noqa: E501

        for task in task_query.iterator():
            queue_task(task)
        found_something = True
    return found_something


def retry_task(task, fg=False):
    task.update(
        status=models.Task.STATUS_PENDING,
        error='',
        broken_reason='',
        log='',
    )
    logger.info("Retrying %r", task)
    task.save()

    if fg:
        col = collections.from_object(task)
        laterz_snoop_task(col.name, task.pk, raise_exceptions=True)
    else:
        queue_task(task)


def retry_tasks(queryset):
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
            first_batch = batch
            logger.info('Queueing first %s tasks...', len(first_batch))
            for task in first_batch:
                queue_task(task)

        progress = int(100.0 * (i / len(all_tasks)))
        logger.info('%s%% done' % (progress,))

    logger.info('100% done submitting tasks.')


def require_dependency(name, depends_on, callback):
    if name in depends_on:
        result = depends_on[name]
        if isinstance(result, Exception):
            raise result
        return result

    task = callback()
    raise MissingDependency(name, task)


@snoop_task('do_nothing')
def do_nothing(name):
    pass


def returns_json_blob(func):

    def wrapper(*args, **kwargs):
        rv = func(*args, **kwargs)

        data = json.dumps(rv, indent=2).encode('utf8')
        with models.Blob.create() as output:
            output.write(data)

        return output.blob

    return wrapper


def dispatch_walk_tasks():
    from .filesystem import walk
    root = models.Directory.root()
    assert root, "root document not created"
    walk.laterz(root.pk)


def save_collection_stats():
    from snoop.data.admin import get_stats
    t0 = time()
    s, _ = models.Statistics.objects.get_or_create(key='stats')
    stats = get_stats()
    for row in stats['task_matrix']:
        for stat in row[1]:
            row[1][stat] = str(row[1][stat])
    s.value = stats
    s.save()
    logger.info('stats for collection {} saved in {} seconds'.format(collections.current().name, time() - t0))  # noqa: E501


def get_rabbitmq_queue_length(q):
    from pyrabbit.api import Client

    cl = Client(settings.SNOOP_RABBITMQ_HTTP_URL, 'guest', 'guest')
    return cl.get_queue_depth('/', q)


def single_task_running(key):
    def count_tasks(method, routing_key):
        count = 0
        inspector = inspect(celery.app)
        task_list_map = inspector.call(method=method, arguments={})
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

    if get_rabbitmq_queue_length(key) > 0:
        return False

    return 1 >= count_tasks('active', routing_key=key) and \
        0 == count_tasks('scheduled', routing_key=key) and \
        0 == count_tasks('reserved', routing_key=key)


@celery.app.task
def save_stats():
    if not single_task_running('save_stats'):
        logger.warning('save_stats function already running, exiting')
        return

    for collection in collections.ALL.values():
        with collection.set_current():
            try:
                if collection.process or \
                        not models.Statistics.objects.filter(key='stats').exists():
                    save_collection_stats()
            except Exception as e:
                logger.exception(e)

    # Kill a little bit of time, in case there are a lot of older
    # messages queued up, they have time to fail in the above
    # check.
    sleep(5)


@celery.app.task
def run_dispatcher():
    if not single_task_running('run_dispatcher'):
        logger.warning('run_dispatcher function already running, exiting')
        return

    for collection in collections.ALL.values():
        logger.info(f'{"=" * 10} collection "{collection.name}" {"=" * 10}')
        try:
            dispatch_for(collection)
        except Exception as e:
            logger.exception(e)

    sleep(5)


def dispatch_for(collection):
    if not collection.process:
        logger.info(f'dispatch: skipping "{collection}", configured with "process = False"')
        return

    queue_len = get_rabbitmq_queue_length(collection.queue_name)
    if queue_len > 0:
        logger.info(f'dispatch: skipping "{collection}", already has {queue_len} queued tasks')
        return

    logger.info('Dispatching for %r', collection)
    from .ocr import dispatch_ocr_tasks

    with collection.set_current():
        if dispatch_tasks(models.Task.STATUS_PENDING):
            logger.info('%r found PENDING tasks, exiting...', collection)
            return True

        if dispatch_tasks(models.Task.STATUS_DEFERRED):
            logger.info('%r found DEFERRED tasks, exiting...', collection)
            return True

        count_before = models.Task.objects.count()
        dispatch_walk_tasks()
        dispatch_ocr_tasks()
        count_after = models.Task.objects.count()
        if count_before != count_after:
            logger.info('%r initial dispatch added new tasks, exiting...', collection)
            return True

        if collection.sync:
            logger.info("sync: retrying all walk tasks")
            # retry up oldest non-pending walk tasks that are older than 1 min
            retry_tasks(
                models.Task.objects
                .filter(func__in=['filesystem.walk', 'ocr.walk_source'])
                .filter(date_modified__lt=timezone.now() - timedelta(minutes=1))
                .exclude(status=models.Task.STATUS_PENDING)
                .order_by('date_modified')[:settings.SYNC_RETRY_LIMIT]
            )

        # retry old errors, don't exit before running sync too
        error_date = timezone.now() - timedelta(days=settings.TASK_RETRY_AFTER_DAYS)
        old_error_qs = (
            models.Task.objects
            .filter(status__in=[models.Task.STATUS_BROKEN, models.Task.STATUS_ERROR])
            .filter(date_modified__lt=error_date)
            .order_by('date_modified')[:settings.SYNC_RETRY_LIMIT]
        )
        if old_error_qs.exists():
            logger.info(f'{collection} found {old_error_qs.count()} ERROR|BROKEN tasks to retry')
            retry_tasks(old_error_qs)

    logger.info(f'dispatch for collection "{collection.name}" done\n')
