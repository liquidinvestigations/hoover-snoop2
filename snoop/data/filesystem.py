import json
from pathlib import Path
from django.utils import timezone
from .utils import time_from_unix
from . import models
from .tasks import shaorma, MissingDependency
from .analyzers import archives
from .analyzers import emlx
from .analyzers import email
from . import digests


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

    for thing in path.iterdir():
        if thing.is_dir():
            (child_directory, _) = directory.child_directory_set.get_or_create(
                collection=directory.collection,
                name=thing.name,
            )
            walk.laterz(child_directory.pk)

        else:
            walk_file.laterz(directory.pk, thing.name)


@shaorma('filesystem.walk_file')
def walk_file(directory_pk, name):
    directory = models.Directory.objects.get(pk=directory_pk)
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

    handle_file.laterz(file.pk)


@shaorma('filesystem.handle_file')
def handle_file(file_pk, **depends_on):
    file = models.File.objects.get(pk=file_pk)
    file.blob = file.original

    if archives.is_archive(file.original.mime_type):
        unarchive_task = archives.unarchive.laterz(file.blob)
        create_archive_files.laterz(
            file.pk,
            depends_on={'archive_listing': unarchive_task},
        )

    elif file.original.mime_type == "application/vnd.ms-outlook":
        eml = depends_on.get('msg_to_eml')
        if not eml:
            task = email.msg_to_eml.laterz(file.original)
            raise MissingDependency('msg_to_eml', task)
        file.blob = eml

    elif file.original.mime_type == 'message/x-emlx':
        eml = depends_on.get('emlx_reconstruct')
        if not eml:
            task = emlx.reconstruct.laterz(file.pk)
            raise MissingDependency('emlx_reconstruct', task)
        file.blob = eml

    if file.blob.mime_type == 'message/rfc822':
        email_parse_task = email.parse.laterz(file.blob)
        create_attachment_files.laterz(
            file.pk,
            depends_on={'email_parse': email_parse_task},
        )

    file.save()

    digests.launch.laterz(file.blob, file.collection.pk)


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

        (file, _) = parent_directory.child_file_set.get_or_create(
            name=name,
            defaults=dict(
                collection=parent_directory.collection,
                ctime=archive.ctime,
                mtime=archive.mtime,
                size=size,
                original=original,
            ),
        )

        return file

    archive = models.File.objects.get(pk=file_pk)
    (fake_root, _) = archive.child_directory_set.get_or_create(
        name='',
        defaults=dict(
            collection=archive.collection,
        ),
    )
    create_directory_children(fake_root, archive_listing_data)


@shaorma('filesystem.create_attachment_files')
def create_attachment_files(file_pk, email_parse):
    def iter_parts(email_data):
        yield email_data
        for part in email_data.get('parts') or []:
            yield from iter_parts(part)

    with email_parse.open() as f:
        email_data = json.load(f)

    attachments = []
    for part in iter_parts(email_data):
        part_attachment = part.get('attachment')
        if part_attachment:
            attachments.append(part_attachment)

    if attachments:
        email_file = models.File.objects.get(pk=file_pk)
        (attachments_dir, _) = email_file.child_directory_set.get_or_create(
            name='',
            defaults=dict(
                collection=email_file.collection,
            ),
        )
        for attachment in attachments:
            original = models.Blob.objects.get(pk=attachment['blob_pk'])
            size = original.path().stat().st_size

            (file, _) = attachments_dir.child_file_set.get_or_create(
                name=attachment['name'],
                defaults=dict(
                    collection=email_file.collection,
                    ctime=email_file.ctime,
                    mtime=email_file.mtime,
                    size=size,
                    original=original,
                ),
            )

            handle_file.laterz(file.pk)
