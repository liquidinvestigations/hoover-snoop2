import logging
import traceback
from django.utils import timezone
from django.db import transaction
from . import celery
from . import models
from .utils import run_once

logger = logging.getLogger(__name__)

shaormerie = {}


class ShaormaError(Exception):

    def __init__(self, message, details):
        super().__init__(message)
        self.details = details


@run_once
def import_shaormas():
    from . import filesystem  # noqa
    from .analyzers import archives  # noqa
    from .analyzers import text  # noqa


@celery.app.task
def laterz_shaorma(task_pk):
    import_shaormas()

    with transaction.atomic():
        task = models.Task.objects.select_for_update().get(pk=task_pk)

        if task.status == models.Task.STATUS_SUCCESS:
            return

        args = task.args
        if task.blob_arg:
            assert args[0] == task.blob_arg.pk
            args = [task.blob_arg] + args[1:]

        kwargs = {dep.name: dep.prev.result for dep in task.prev_set.all()}

        task.status = models.Task.STATUS_PENDING
        task.date_started = timezone.now()
        task.save()

        try:
            result = shaormerie[task.func](*args, **kwargs)

            if result is not None:
                assert isinstance(result, models.Blob)
                task.result = result

            task.status = models.Task.STATUS_SUCCESS

        except Exception as e:
            if isinstance(e, ShaormaError):
                task.error = "{} ({})".format(e.args[0], e.details)

            else:
                task.error = repr(e)

            task.status = models.Task.STATUS_ERROR
            task.traceback = traceback.format_exc()
            logger.error("Shaorma %d failed: %s", task_pk, task.error)

        else:
            task.error = ''
            task.traceback = ''

        task.date_finished = timezone.now()
        task.save()

    if task.status == models.Task.STATUS_SUCCESS:
        for next_dependency in task.next_set.all():
            next = next_dependency.next
            laterz_shaorma.delay(next.pk)


def shaorma(name):
    def decorator(func):
        def laterz(*args, depends_on={}):
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
                return task

            if depends_on:
                all_done = True
                for dep_name, dep in depends_on.items():
                    dep = type(dep).objects.get(pk=dep.pk)  # make DEP grate again
                    if dep.result is None:
                        all_done = False
                    models.TaskDependency.objects.get_or_create(
                        prev=dep,
                        next=task,
                        name=dep_name,
                    )

                if all_done:
                    laterz_shaorma.delay(task.pk)

            else:
                laterz_shaorma.delay(task.pk)

            return task

        func.laterz = laterz
        shaormerie[name] = func
        return func

    return decorator


def dispatch_pending_tasks():
    for task in models.Task.objects.filter(status=models.Task.STATUS_PENDING):
        deps_not_ready = (
            task.prev_set
            .exclude(prev__status=models.Task.STATUS_SUCCESS)
            .exists()
        )
        if deps_not_ready:
            continue
        logger.debug("Dispatching %r", task)
        laterz_shaorma.delay(task.pk)
