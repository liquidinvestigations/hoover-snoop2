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


@celery.app.task
def walk(directory_pk):
    directory = models.Directory.objects.get(pk=directory_pk)
    path = directory_absolute_path(directory)
    print('path:', path)
    for thing in path.iterdir():
        print(thing)
        if thing.is_dir():
            print('dir')
            (child_directory, _) = directory.child_directory_set.get_or_create(collection=directory.collection, name=thing.name)
            walk.delay(child_directory.pk)
        else:
            print('file')
            file_to_blob.delay(directory_pk, thing.name)


def chunks(file, blocksize=65536):
    while True:
        data = file.read(blocksize)
        if not data:
            return
        yield data


@celery.app.task
def file_to_blob(directory_pk, name):
    directory = models.Directory.objects.get(pk=directory_pk)
    path = directory_absolute_path(directory) / name
    print('making blob', path)

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
            digest = {name: hash.hexdigest() for name, hash in hashes.items()}
            b.set_filename(digest['sha3_256'])

    blob, blob_created = models.Blob.objects.get_or_create(
        sha3_256=digest['sha3_256'],
        defaults=dict(
            sha1=digest['sha1'],
            sha256=digest['sha256'],
            md5=digest['md5'],
            magic='',
            mime_type=magic.mime_type,
            mime_encoding=magic.mime_encoding,
        )
    )

    print(blob)
