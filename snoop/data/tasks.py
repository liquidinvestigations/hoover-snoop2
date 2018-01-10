import logging
from django.utils import timezone
from . import celery
from . import models
from .utils import run_once

logger = logging.getLogger(__name__)

shaormerie = {}


@run_once
def import_shaormas():
    from . import filesystem  # noqa
    from .analyzers import archives  # noqa
    from .analyzers import text  # noqa


@celery.app.task
def laterz_shaorma(task_pk):
    import_shaormas()

    task = models.Task.objects.get(pk=task_pk)

    args = task.args
    kwargs = {dep.name: dep.prev.result for dep in task.prev_set.all()}

    task.date_started = timezone.now()
    task.save()

    result = shaormerie[task.func](*args, **kwargs)
    task.date_finished = timezone.now()

    if result is not None:
        assert isinstance(result, models.Blob)
        task.result = result

    task.save()

    for next_dependency in task.next_set.all():
        next = next_dependency.next
        laterz_shaorma.delay(next.pk)


def shaorma(func):
    def laterz(*args, depends_on={}):
        task, _ = models.Task.objects.get_or_create(
            func=func.__name__,
            args=args,
        )

        if task.date_finished:
            return task

        if depends_on:
            all_done = True
            for name, dep in depends_on.items():
                dep = type(dep).objects.get(pk=dep.pk)  # make DEP grate again
                if dep.result is None:
                    all_done = False
                models.TaskDependency.objects.get_or_create(
                    prev=dep,
                    next=task,
                    name=name,
                )

            if all_done:
                laterz_shaorma.delay(task.pk)

        else:
            laterz_shaorma.delay(task.pk)

        return task

    func.laterz = laterz
    shaormerie[func.__name__] = func
    return func
