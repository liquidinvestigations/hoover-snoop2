import json
from datetime import datetime
import subprocess
import tempfile
import logging
import hashlib
from pathlib import Path
from django.conf import settings
from . import celery
from . import models
from .blobs import FlatBlobStorage
from .magic import Magic

logger = logging.getLogger(__name__)

blob_storage = FlatBlobStorage(settings.SNOOP_BLOB_STORAGE)

shaormerie = {}


@celery.app.task
def laterz_shaorma(task_pk):
    task = models.Task.objects.get(pk=task_pk)

    args = json.loads(task.args)
    kwargs = {dep.name: dep.prev.result for dep in task.prev_set.all()}

    result = shaormerie[task.func](*args, **kwargs)

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
        if depends_on:
            all_done = True
            for name, dep in depends_on.items():
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


def chunks(file, blocksize=65536):
    while True:
        data = file.read(blocksize)
        if not data:
            return
        yield data


def make_blob_from_file(path):
    hashes = {
        'md5': hashlib.md5(),
        'sha1': hashlib.sha1(),
        'sha3_256': hashlib.sha3_256(),
        'sha256': hashlib.sha256(),
    }

    magic = Magic()

    with blob_storage.save() as b:
        with path.open('rb') as f:
            for block in chunks(f):
                for h in hashes.values():
                    h.update(block)
                magic.update(block)
                b.write(block)

            magic.finish()
            hexdigest = {
                name: hash.hexdigest()
                for name, hash in hashes.items()
            }
            b.set_filename(hexdigest['sha3_256'])

    blob, blob_created = models.Blob.objects.get_or_create(
        sha3_256=hexdigest['sha3_256'],
        defaults=dict(
            sha1=hexdigest['sha1'],
            sha256=hexdigest['sha256'],
            md5=hexdigest['md5'],
            magic='',
            mime_type=magic.mime_type,
            mime_encoding=magic.mime_encoding,
        )
    )

    return blob


@shaorma
def file_to_blob(directory_pk, name):
    directory = models.Directory.objects.get(pk=directory_pk)
    path = directory_absolute_path(directory) / name
    blob = make_blob_from_file(path)

    stat = path.stat()
    file, _ = directory.child_file_set.get_or_create(
        name=name,
        defaults=dict(
            collection=directory.collection,
            ctime=datetime.utcfromtimestamp(stat.st_ctime),
            mtime=datetime.utcfromtimestamp(stat.st_mtime),
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
    with blob_storage.open(archive_listing.pk) as f:
        archive_listing_data = json.load(f)

    def create_directory_children(directory, children):
        print(f'children: {children}')
        for item in children:
            print(f'item: {item}')
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
        size = blob_storage.path(blob.pk).stat().st_size
        now = datetime.utcnow()

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
def handle_file(file_pk):
    file = models.File.objects.get(pk=file_pk)
    blob = file.blob

    if blob.mime_type in SEVENZIP_KNOWN_TYPES:
        unarchive_task = unarchive.laterz(blob.pk)
        create_archive_files.laterz(
            file.pk,
            depends_on={'archive_listing': unarchive_task},
        )


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
                'blob_pk': make_blob_from_file(thing).pk,
            }


@shaorma
def unarchive(blob_pk):
    with tempfile.TemporaryDirectory() as temp_dir:
        call_7z(blob_storage.path(blob_pk), temp_dir)
        listing = list(archive_walk(Path(temp_dir)))

    with tempfile.NamedTemporaryFile() as f:
        f.write(json.dumps(listing).encode('utf-8'))
        f.flush()
        listing_blob = make_blob_from_file(Path(f.name))

    return listing_blob
