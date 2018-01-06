import json
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
    for thing in path.iterdir():
        if thing.is_dir():
            (child_directory, _) = directory.child_directory_set.get_or_create(collection=directory.collection, name=thing.name)
            walk.delay(child_directory.pk)
        else:
            file_to_blob.delay(directory_pk, thing.name)


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
            hexdigest = {name: hash.hexdigest() for name, hash in hashes.items()}
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


@celery.app.task
def file_to_blob(directory_pk, name):
    directory = models.Directory.objects.get(pk=directory_pk)
    path = directory_absolute_path(directory) / name
    #print('making blob', path)

    blob = make_blob_from_file(path)

    digest.delay(blob.pk)


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


@celery.app.task
def digest(blob_pk):
    blob = models.Blob.objects.get(pk=blob_pk)

    if blob.mime_type in SEVENZIP_KNOWN_TYPES:
        unarchive.delay(blob_pk)

def archive_walk(path):
    for thing in path.iterdir():
        if thing.is_dir():
            yield from archive_walk(thing)
        else:
            yield (thing, make_blob_from_file(thing).pk)

@celery.app.task
def unarchive(blob_pk):
    with tempfile.TemporaryDirectory() as temp_dir:
        call_7z(blob_storage.path(blob_pk), temp_dir)

        listing = [
            {"path": str(path.relative_to(temp_dir)), "blob_pk": pk}
            for path, pk in archive_walk(Path(temp_dir))
        ]

    with tempfile.NamedTemporaryFile() as f:
        f.write(json.dumps(listing).encode('utf-8'))
        f.flush()
        listing_blob = make_blob_from_file(Path(f.name))

    print("listing at " + listing_blob.pk)
