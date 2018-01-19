import json
from pathlib import Path
from django.utils import timezone
from datetime import datetime
from . import models
from .tasks import shaorma
from .analyzers import archives
from .analyzers import tika
from .analyzers import emlx
from .analyzers import email
from . import digests


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


@shaorma('filesystem.walk')
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
    original = models.Blob.create_from_file(path)

    stat = path.stat()
    file, _ = directory.child_file_set.get_or_create(
        name=name,
        defaults=dict(
            collection=directory.collection,
            ctime=time_from_unix(stat.st_ctime),
            mtime=time_from_unix(stat.st_mtime),
            size=stat.st_size,
            original=original,
        ),
    )

    return file


@shaorma('filesystem.handle_file')
def handle_file(file_pk):
    file = models.File.objects.get(pk=file_pk)
    depends_on = {}

    if archives.is_archive(file.original.mime_type):
        file.blob = file.original
        unarchive_task = archives.unarchive.laterz(file.blob)
        create_archive_files.laterz(
            file.pk,
            depends_on={'archive_listing': unarchive_task},
        )

    elif file.original.mime_type == "application/vnd.ms-outlook":
        file.blob = email.msg_blob_to_eml(file.original)
        depends_on['email_parse'] = email.parse.laterz(file.blob.pk)

    elif file.original.mime_type == 'message/x-emlx':
        file.blob = emlx.reconstruct(file)
        depends_on['email_parse'] = email.parse.laterz(file.blob)

    elif file.original.mime_type == 'message/rfc822':
        file.blob = file.original
        depends_on['email_parse'] = email.parse.laterz(file.blob)

    elif tika.can_process(file.original):
        file.blob = file.original
        depends_on['tika_rmeta'] = tika.rmeta.laterz(file.blob)

    else:
        file.blob = file.original

    file.save()

    digests.compute.laterz(
        file.blob,
        file.collection.pk,
        depends_on=depends_on,
    )


@shaorma('filesystem.create_archive_files')
def create_archive_files(file_pk, archive_listing):
    with archive_listing.open() as f:
        archive_listing_data = json.load(f)

    def create_directory_children(directory, children):
        child_files = []
        for item in children:
            if item['type'] == 'file':
                child_original = models.Blob.objects.get(pk=item['blob_pk'])
                file = create_file(directory, item['name'], child_original)
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

    def create_file(parent_directory, name, original):
        size = original.path().stat().st_size
        now = timezone.now()

        (file, _) = parent_directory.child_file_set.get_or_create(
            name=name,
            defaults=dict(
                collection=parent_directory.collection,
                ctime=now,
                mtime=now,
                size=size,
                original=original,
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
