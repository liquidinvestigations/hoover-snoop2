import json
from time import time
import logging
from io import StringIO
from contextlib import contextmanager
from django.utils import timezone
from django.db import transaction
from . import celery
from . import models
from .utils import run_once

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
    def send_to_celery():
        laterz_shaorma.apply_async((task.pk,), queue=task.func)

    transaction.on_commit(send_to_celery)


def queue_next_tasks(task, reset=False):
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


def is_competed(task):
    COMPLETED = [models.Task.STATUS_SUCCESS,  models.Task.STATUS_BROKEN]
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

        if is_competed(task):
            logger.info("%r already completed", task)
            queue_next_tasks(task)
            return

        args = task.args
        if task.blob_arg:
            assert args[0] == task.blob_arg.pk
            args = [task.blob_arg] + args[1:]

        depends_on = {}
        for dep in task.prev_set.all():
            prev_task = dep.prev
            if not is_competed(prev_task):
                task.update(
                    status=models.Task.STATUS_DEFERRED,
                    error='',
                    broken_reason='',
                    log=handler.stream.getvalue(),
                )
                task.save()
                logger.info("%r missing dependency %r", task, prev_task)
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

        task.status = models.Task.STATUS_PENDING
        task.date_started = timezone.now()
        task.save()

        logger.info("Running %r", task)
        t0 = time()
        try:
            result = shaormerie[task.func](*args, **depends_on)

            if result is not None:
                assert isinstance(result, models.Blob)
                task.result = result

        except MissingDependency as dep:
            logger.info(
                "%r requests an extra dependency: %r [%.03f s]",
                task, dep, time() - t0,
            )

            task.update(
                status=models.Task.STATUS_DEFERRED,
                error='',
                broken_reason='',
                log=handler.stream.getvalue(),
            )
            models.TaskDependency.objects.get_or_create(
                prev=dep.task,
                next=task,
                name=dep.name,
            )
            queue_task(task)

        except ShaormaBroken as e:
            logger.exception(
                "%r broken: %s [%.03f s]",
                task, task.error, time() - t0,
            )
            task.update(
                status=models.Task.STATUS_BROKEN,
                error="{}: {}".format(e.reason, e.args[0]),
                broken_reason=e.reason,
                log=handler.stream.getvalue(),
            )

        except Exception as e:
            if raise_exceptions:
                raise

            if isinstance(e, ShaormaError):
                error = "{} ({})".format(e.args[0], e.details)
            else:
                error = repr(e)

            logger.exception(
                "%r failed: %s [%.03f s]",
                task, task.error, time() - t0,
            )
            task.update(
                status=models.Task.STATUS_ERROR,
                error=error,
                broken_reason='',
                log=handler.stream.getvalue(),
            )

        else:
            logger.info("%r succeeded [%.03f s]", task, time() - t0)
            task.update(
                status=models.Task.STATUS_SUCCESS,
                error='',
                broken_reason='',
                log=handler.stream.getvalue(),
            )

        task.date_finished = timezone.now()
        task.save()

    if is_competed(task):
        queue_next_tasks(task, reset=True)


def shaorma(name):
    def decorator(func):
        def laterz(*args, depends_on={}, retry=False):
            if args and isinstance(args[0], models.Blob):
                blob_arg = args[0]
                args = (blob_arg.pk,) + args[1:]

            else:
                blob_arg = None

            task, _ = models.Task.objects.get_or_create(
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

            queue_task(task)

            return task

        func.laterz = laterz
        shaormerie[name] = func
        return func

    return decorator


def dispatch_pending_tasks():
    task_query = (
        models.Task.objects
        .filter(status__in=[
            models.Task.STATUS_PENDING,
            models.Task.STATUS_DEFERRED,
        ])
    )

    for task in task_query:
        deps_not_ready = (
            task.prev_set
            .exclude(prev__status=models.Task.STATUS_SUCCESS)
            .exists()
        )
        if deps_not_ready:
            continue
        logger.info("Dispatching %r", task)
        queue_task(task)


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
    for task in queryset.iterator():
        retry_task(task)


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
