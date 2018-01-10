import json
import subprocess
import tempfile
import logging
from pathlib import Path
from django.utils import timezone
from . import celery
from . import models
from .utils import run_once

logger = logging.getLogger(__name__)

shaormerie = {}


@run_once
def import_shaormas():
    from . import filesystem  # noqa


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


SEVENZIP_KNOWN_TYPES = {
    'application/zip',
    'application/rar',
    'application/x-7z-compressed',
    'application/x-zip',
    'application/x-gzip',
    'application/x-zip-compressed',
    'application/x-rar-compressed',
}


def call_7z(archive_path, output_dir):
    subprocess.check_output([
        '7z',
        '-y',
        '-pp',
        'x',
        str(archive_path),
        '-o' + str(output_dir),
    ], stderr=subprocess.STDOUT)


@shaorma
def extract_text(blob_pk):
    blob = models.Blob.objects.get(pk=blob_pk)

    with models.Blob.create() as output:
        with blob.open() as src:
            output.write(src.read())

    return output.blob


def archive_walk(path):
    for thing in path.iterdir():
        if thing.is_dir():
            yield {
                'type': 'directory',
                'name': thing.name,
                'children': list(archive_walk(thing)),
            }

        else:
            yield {
                'type': 'file',
                'name': thing.name,
                'blob_pk': models.Blob.create_from_file(thing).pk,
            }


@shaorma
def unarchive(blob_pk):
    with tempfile.TemporaryDirectory() as temp_dir:
        call_7z(models.Blob.objects.get(pk=blob_pk).path(), temp_dir)
        listing = list(archive_walk(Path(temp_dir)))

    with tempfile.NamedTemporaryFile() as f:
        f.write(json.dumps(listing).encode('utf-8'))
        f.flush()
        listing_blob = models.Blob.create_from_file(Path(f.name))

    return listing_blob
