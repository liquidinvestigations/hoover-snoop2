from contextlib import contextmanager
from io import StringIO
import json
import logging
from time import time

from celery.bin.control import inspect
from django.conf import settings
from django.db import transaction
from django.utils import timezone

from . import celery
from . import models
from ..profiler import profile
from .utils import run_once
from requests.exceptions import ConnectionError
from snoop import tracing

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

shaormerie = {}


class ShaormaError(Exception):

    def __init__(self, message, details):
        super().__init__(message)
        self.details = details


class ShaormaBroken(Exception):

    def __init__(self, message, reason):
        super().__init__(message)
        self.reason = reason


class MissingDependency(Exception):

    def __init__(self, name, task):
        self.name = name
        self.task = task


def queue_task(task):
    import_shaormas()

    def send_to_celery():
        laterz_shaorma.apply_async(
            (task.pk,),
            queue=f'{settings.TASK_PREFIX}.{task.func}',
            priority=shaormerie[task.func].priority)

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
def import_shaormas():
    from . import filesystem  # noqa
    from .analyzers import archives  # noqa
    from .analyzers import text  # noqa


def is_completed(task):
    COMPLETED = [models.Task.STATUS_SUCCESS, models.Task.STATUS_BROKEN]
    return task.status in COMPLETED


@contextmanager
def shaorma_log_handler(level=logging.DEBUG):
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
def laterz_shaorma(task_pk, raise_exceptions=False):
    import_shaormas()
    with transaction.atomic(), shaorma_log_handler() as handler:
        task = models.Task.objects.select_for_update().get(pk=task_pk)
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
            for dep in task.prev_set.all():
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
                    return

                if prev_task.status == models.Task.STATUS_SUCCESS:
                    prev_result = prev_task.result
                elif prev_task.status == models.Task.STATUS_BROKEN:
                    prev_result = ShaormaBroken(
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
                func = shaormerie[task.func]
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
                    models.TaskDependency.objects.get_or_create(
                        prev=dep.task,
                        next=task,
                        name=dep.name,
                    )
                    queue_task(task)

            except ShaormaBroken as e:
                msg = '%r broken: %s [%.03f s]' % (task, task.error, time() - t0)
                logger.exception(msg)
                tracing.add_annotation(msg)

                task.update(
                    status=models.Task.STATUS_BROKEN,
                    error="{}: {}".format(e.reason, e.args[0]),
                    broken_reason=e.reason,
                    log=log_handler.stream.getvalue(),
                )

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
                msg = '%r failed: %s [%.03f s]' % (task, task.error, time() - t0)
                tracing.add_annotation(msg)

                if raise_exceptions:
                    raise

                if isinstance(e, ShaormaError):
                    error = "{} ({})".format(e.args[0], e.details)
                else:
                    error = repr(e)

                logger.exception(msg)
                task.update(
                    status=models.Task.STATUS_ERROR,
                    error=error,
                    broken_reason='',
                    log=log_handler.stream.getvalue(),
                )

            else:
                logger.info("%r succeeded [%.03f s]", task, time() - t0)
                task.update(
                    status=models.Task.STATUS_SUCCESS,
                    error='',
                    broken_reason='',
                    log=log_handler.stream.getvalue(),
                )

        with tracing.span('save state after run'):
            task.date_finished = timezone.now()
            task.save()

    if is_completed(task):
        queue_next_tasks(task, reset=True)


def shaorma(name, priority=5):

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

            if task.date_finished:
                if retry:
                    retry_task(task)
                return task

            if depends_on:
                for dep_name, dep in depends_on.items():
                    models.TaskDependency.objects.get_or_create(
                        prev=dep,
                        next=task,
                        name=dep_name,
                    )

            if queue_now:
                queue_task(task)

            return task

        func.laterz = laterz
        func.priority = priority
        shaormerie[name] = func
        return func

    return decorator


def dispatch_tasks(status):
    task_query = (
        models.Task.objects
        .filter(status=status)
        .order_by('-date_modified')  # newest pending tasks first
    )[:settings.DISPATCH_QUEUE_LIMIT]

    task_count = task_query.count()
    if not task_count:
        logger.info('No tasks to dispatch')
        return False
    logger.info('Dispatching remaining %s tasks.', task_count)

    for task in task_query.iterator():
        deps_not_ready = (
            task.prev_set
            .exclude(prev__status=models.Task.STATUS_SUCCESS)
            .exists()
        )
        if deps_not_ready:
            logger.info('Cannot dispatch %r, deps not ready, settings as DEFERRED', task)
            task.update(
                status=models.Task.STATUS_DEFERRED,
                error='',
                broken_reason='',
                log="deps not ready",
            )
            task.save()

            logger.info('Dispatching pending dependencies...')
            for dep in task.prev_set.filter(prev__status=models.Task.STATUS_PENDING).iterator():
                queue_task(dep.prev)
            else:
                logger.error("No pending tasks for DEFERRED")
            continue

        logger.info("Dispatching %r", task)
        queue_task(task)

    logger.info('Done dispatching pending tasks!')
    return True


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
        laterz_shaorma(task.pk, raise_exceptions=True)
    else:
        queue_task(task)


def retry_tasks(queryset):
    logger.info('Retrying %s tasks...', queryset.count())
    for task in queryset.iterator():
        retry_task(task)
    logger.info('Done submitting %s tasks.', queryset.count())


def require_dependency(name, depends_on, callback):
    if name in depends_on:
        result = depends_on[name]
        if isinstance(result, Exception):
            raise result
        return result

    task = callback()
    raise MissingDependency(name, task)


@shaorma('do_nothing')
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


def count_tasks(tasks_status, excluded=[]):
    if not tasks_status:
        return 0

    count = 0
    for tasks in tasks_status.values():
        for task in tasks:
            if task['name'] not in excluded:
                count += 1
    return count


def has_any_tasks():
    excluded = ['snoop.data.tasks.run_dispatcher']

    inspector = inspect(celery.app)
    active = inspector.call(method='active', arguments={})
    scheduled = inspector.call(method='scheduled', arguments={})
    reserved = inspector.call(method='reserved', arguments={})
    count = (
        count_tasks(active, excluded)
        + count_tasks(scheduled, excluded)
        + count_tasks(reserved, excluded)
    )
    logger.info('has_any_tasks found %s active tasks', count)
    return count > 0


def dispatch_walk_tasks():
    from .filesystem import walk
    root = models.Directory.root()
    if not root:
        root = models.Directory.objects.create()
    walk.laterz(root.pk)


@celery.app.task
def run_dispatcher():
    from .ocr import dispatch_ocr_tasks

    if has_any_tasks():
        logger.info('skipping run_dispatcher -- already have tasks')
        return
    logger.info('running run_dispatcher')

    if dispatch_tasks(models.Task.STATUS_PENDING):
        logger.info('found PENDING tasks, exiting...')
        return

    if dispatch_tasks(models.Task.STATUS_DEFERRED):
        logger.info('found DEFERRED tasks, exiting...')
        return

    count_before = models.Task.objects.filter().count()
    dispatch_walk_tasks()
    dispatch_ocr_tasks()
    count_after = models.Task.objects.filter().count()
    if count_before != count_after:
        logger.info('initial dispatch added new tasks, exiting...')
        return

    if settings.SYNC_FILES:
        logger.info("sync: retrying all walk tasks")
        queryset = (
            models.Task.objects
            .filter(func__in=['filesystem.walk', 'ocr.walk_source'])
            .order_by('date_modified')[:settings.DISPATCH_QUEUE_LIMIT]
        )
        retry_tasks(queryset)
