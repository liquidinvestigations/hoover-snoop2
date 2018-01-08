import json
from datetime import datetime
import subprocess
import tempfile
import logging
from pathlib import Path
from django.utils import timezone
from . import celery
from . import models

logger = logging.getLogger(__name__)

shaormerie = {}


@celery.app.task
def laterz_shaorma(task_pk):
    task = models.Task.objects.get(pk=task_pk)

    args = json.loads(task.args)
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
            args=json.dumps(args, sort_keys=True),
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


def time_from_unix(t):
    return timezone.utc.fromutc(datetime.utcfromtimestamp(t))


def directory_absolute_path(directory):
    path_elements = []
    node = directory
    path = Path(directory.collection.root)

    while node.parent_directory:
        path_elements.append(node.name)
        node = node.parent_directory
    for name in reversed(path_elements):
        path /= name

    return path


@shaorma
def walk(directory_pk):
    directory = models.Directory.objects.get(pk=directory_pk)
    path = directory_absolute_path(directory)
    for thing in path.iterdir():
        if thing.is_dir():
            (child_directory, _) = directory.child_directory_set.get_or_create(
                collection=directory.collection,
                name=thing.name,
            )
            walk.laterz(child_directory.pk)
        else:
            file_to_blob.laterz(directory_pk, thing.name)



@shaorma
def file_to_blob(directory_pk, name):
    directory = models.Directory.objects.get(pk=directory_pk)
    path = directory_absolute_path(directory) / name
    blob = models.Blob.create_from_file(path)

    stat = path.stat()
    file, _ = directory.child_file_set.get_or_create(
        name=name,
        defaults=dict(
            collection=directory.collection,
            ctime=time_from_unix(stat.st_ctime),
            mtime=time_from_unix(stat.st_mtime),
            size=stat.st_size,
            blob=blob,
        ),
    )

    handle_file.laterz(file.pk)


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
def create_archive_files(file_pk, archive_listing):
    with archive_listing.open() as f:
        archive_listing_data = json.load(f)

    def create_directory_children(directory, children):
        for item in children:
            if item['type'] == 'file':
                blob = models.Blob.objects.get(pk=item['blob_pk'])
                create_file(directory, item['name'], blob)

            if item['type'] == 'directory':
                create_directory(directory, item['name'], item['children'])

    def create_directory(parent_directory, name, children):
        (directory, _) = parent_directory.child_directory_set.get_or_create(
            name=name,
            defaults=dict(
                collection=parent_directory.collection,
            ),
        )
        create_directory_children(directory, children)

    def create_file(parent_directory, name, blob):
        size = blob.path().stat().st_size
        now = timezone.now()

        parent_directory.child_file_set.get_or_create(
            name=name,
            defaults=dict(
                collection=parent_directory.collection,
                ctime=now,
                mtime=now,
                size=size,
                blob=blob,
            ),
        )

    file = models.File.objects.get(pk=file_pk)
    (fake_root, _) = file.child_directory_set.get_or_create(
        name='',
        defaults=dict(
            collection=file.collection,
        ),
    )
    create_directory_children(fake_root, archive_listing_data)


@shaorma
def extract_text(blob_pk):
    blob = models.Blob.objects.get(pk=blob_pk)

    with models.Blob.create() as output:
        with blob.open() as src:
            output.write(src.read())

    return output.blob


@shaorma
def handle_file(file_pk):
    file = models.File.objects.get(pk=file_pk)
    blob = file.blob
    depends_on = {}

    if blob.mime_type in SEVENZIP_KNOWN_TYPES:
        unarchive_task = unarchive.laterz(blob.pk)
        create_archive_files.laterz(
            file.pk,
            depends_on={'archive_listing': unarchive_task},
        )

    if blob.mime_type == 'text/plain':
        depends_on['text'] = extract_text.laterz(blob.pk)

    digest.laterz(file.collection.pk, blob.pk, depends_on=depends_on)


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


@shaorma
def digest(collection_pk, blob_pk, **depends_on):
    collection = models.Collection.objects.get(pk=collection_pk)
    blob = models.Blob.objects.get(pk=blob_pk)

    rv = {}
    text_blob = depends_on.get('text')
    if text_blob:
        with text_blob.open() as f:
            text_bytes = f.read()
        rv['text'] = text_bytes.decode(text_blob.mime_encoding)

    with models.Blob.create() as writer:
        writer.write(json.dumps(rv).encode('utf-8'))

    collection.digest_set.get_or_create(blob=blob, result=writer.blob)
