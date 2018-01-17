import json
from pathlib import Path
from django.utils import timezone
from datetime import datetime
from . import models
from .tasks import shaorma
from .analyzers import archives
from .analyzers import text
from .analyzers import tika


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
    child_files = []
    for thing in path.iterdir():
        if thing.is_dir():
            (child_directory, _) = directory.child_directory_set.get_or_create(
                collection=directory.collection,
                name=thing.name,
            )
            walk.laterz(child_directory.pk)
        else:
            file = file_to_blob(directory, thing.name)
            child_files.append(file)

    for file in child_files:
        handle_file.laterz(file.pk)


def file_to_blob(directory, name):
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

    return file


@shaorma
def handle_file(file_pk):
    file = models.File.objects.get(pk=file_pk)
    blob = file.blob
    depends_on = {}

    if blob.mime_type in archives.SEVENZIP_KNOWN_TYPES:
        unarchive_task = archives.unarchive.laterz(blob.pk)
        create_archive_files.laterz(
            file.pk,
            depends_on={'archive_listing': unarchive_task},
        )

    if tika.can_process(blob):
        depends_on['tika_rmeta'] = tika.rmeta.laterz(blob.pk)

    digest.laterz(file.collection.pk, blob.pk, depends_on=depends_on)


@shaorma
def create_archive_files(file_pk, archive_listing):
    with archive_listing.open() as f:
        archive_listing_data = json.load(f)

    def create_directory_children(directory, children):
        child_files = []
        for item in children:
            if item['type'] == 'file':
                blob = models.Blob.objects.get(pk=item['blob_pk'])
                file = create_file(directory, item['name'], blob)
                child_files.append(file)

            if item['type'] == 'directory':
                create_directory(directory, item['name'], item['children'])

        for file in child_files:
            handle_file.laterz(file.pk)

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

        (file, _) = parent_directory.child_file_set.get_or_create(
            name=name,
            defaults=dict(
                collection=parent_directory.collection,
                ctime=now,
                mtime=now,
                size=size,
                blob=blob,
            ),
        )

        return file

    file = models.File.objects.get(pk=file_pk)
    (fake_root, _) = file.child_directory_set.get_or_create(
        name='',
        defaults=dict(
            collection=file.collection,
        ),
    )
    create_directory_children(fake_root, archive_listing_data)


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

    tika_rmeta_blob = depends_on.get('tika_rmeta')
    if tika_rmeta_blob:
        with tika_rmeta_blob.open(encoding='utf8') as f:
            tika_rmeta = json.load(f)
        rv['text'] = tika_rmeta[0]['X-TIKA:content']

    with models.Blob.create() as writer:
        writer.write(json.dumps(rv).encode('utf-8'))

    collection.digest_set.get_or_create(blob=blob, result=writer.blob)
