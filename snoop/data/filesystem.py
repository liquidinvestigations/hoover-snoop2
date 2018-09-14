import json
import logging
from pathlib import Path

from snoop.profiler import profile

from . import digests
from . import models
from .analyzers import archives
from .analyzers import email
from .analyzers import emlx
from .tasks import shaorma, require_dependency, ShaormaBroken
from .utils import time_from_unix

log = logging.getLogger(__name__)


def directory_absolute_path(directory):
    path_elements = []
    node = directory

    if not directory.collection.root:
        raise RuntimeError("Collection root is not set")

    path = Path(directory.collection.root)

    while node.parent_directory:
        path_elements.append(node.name)
        node = node.parent_directory
    for name in reversed(path_elements):
        path /= name

    return path


@shaorma('filesystem.walk')
@profile()
def walk(directory_pk):
    directory = models.Directory.objects.get(pk=directory_pk)
    path = directory_absolute_path(directory)

    new_files = []
    for thing in path.iterdir():
        if thing.is_dir():
            (child_directory, _) = directory.child_directory_set.get_or_create(
                collection=directory.collection,
                name_bytes=thing.name.encode('utf8', errors='surrogateescape'),
            )
            walk.laterz(child_directory.pk)

        else:
            directory = models.Directory.objects.get(pk=directory_pk)
            path = directory_absolute_path(directory) / thing.name
            stat = path.stat()

            # TODO if file is already loaded, and size+mtime are the same,
            # don't re-import it

            original = models.Blob.create_from_file(path)

            file, _ = directory.child_file_set.get_or_create(
                name_bytes=thing.name.encode('utf8', errors='surrogateescape'),
                defaults=dict(
                    collection=directory.collection,
                    ctime=time_from_unix(stat.st_ctime),
                    mtime=time_from_unix(stat.st_mtime),
                    size=stat.st_size,
                    original=original,
                    blob=original,
                ),
            )
            new_files.append(file)

    for file in new_files:
        handle_file.laterz(file.pk)


@shaorma('filesystem.handle_file')
@profile()
def handle_file(file_pk, **depends_on):
    file = models.File.objects.get(pk=file_pk)
    file.blob = file.original

    if archives.is_archive(file.original.mime_type):
        unarchive_task = archives.unarchive.laterz(file.blob)
        create_archive_files.laterz(
            file.pk,
            depends_on={'archive_listing': unarchive_task},
        )

    elif file.original.mime_type in email.OUTLOOK_POSSIBLE_MIME_TYPES:
        try:
            file.blob = require_dependency(
                'msg_to_eml', depends_on,
                lambda: email.msg_to_eml.laterz(file.original),
            )
        except ShaormaBroken:
            pass

    elif file.original.mime_type == 'message/x-emlx':
        file.blob = require_dependency(
            'emlx_reconstruct', depends_on,
            lambda: emlx.reconstruct.laterz(file.pk),
        )

    if file.blob.mime_type == 'message/rfc822':
        email_parse_task = email.parse.laterz(file.blob)
        create_attachment_files.laterz(
            file.pk,
            depends_on={'email_parse': email_parse_task},
        )

    file.save()

    digests.launch.laterz(file.blob, file.collection.pk)


@shaorma('filesystem.create_archive_files')
@profile()
def create_archive_files(file_pk, archive_listing):
    if isinstance(archive_listing, ShaormaBroken):
        log.debug("Unarchive task is broken; returning without doing anything")
        return

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
            name_bytes=name.encode('utf8', errors='surrogateescape'),
            defaults=dict(
                collection=parent_directory.collection,
            ),
        )
        create_directory_children(directory, children)

    def create_file(parent_directory, name, original):
        size = original.path().stat().st_size

        (file, _) = parent_directory.child_file_set.get_or_create(
            name_bytes=name.encode('utf8', errors='surrogateescape'),
            defaults=dict(
                collection=parent_directory.collection,
                ctime=archive.ctime,
                mtime=archive.mtime,
                size=size,
                original=original,
                blob=original,
            ),
        )

        return file

    archive = models.File.objects.get(pk=file_pk)
    (fake_root, _) = archive.child_directory_set.get_or_create(
        name_bytes=b'',
        defaults=dict(
            collection=archive.collection,
        ),
    )
    create_directory_children(fake_root, archive_listing_data)


def get_email_attachments(parsed_email):
    if isinstance(parsed_email, ShaormaBroken):
        log.debug("Email task is broken; returning without doing anything")
        return

    def iter_parts(email_data):
        yield email_data
        for part in email_data.get('parts') or []:
            yield from iter_parts(part)

    with parsed_email.open() as f:
        email_data = json.load(f)

    for part in iter_parts(email_data):
        part_attachment = part.get('attachment')
        if part_attachment:
            yield part_attachment


@shaorma('filesystem.create_attachment_files')
@profile()
def create_attachment_files(file_pk, email_parse):
    attachments = list(get_email_attachments(email_parse))

    if attachments:
        email_file = models.File.objects.get(pk=file_pk)
        (attachments_dir, _) = email_file.child_directory_set.get_or_create(
            name_bytes=b'',
            defaults=dict(
                collection=email_file.collection,
            ),
        )
        for attachment in attachments:
            original = models.Blob.objects.get(pk=attachment['blob_pk'])
            size = original.path().stat().st_size

            name_bytes = (
                attachment['name']
                .encode('utf8', errors='surrogateescape')
            )
            (file, _) = attachments_dir.child_file_set.get_or_create(
                name_bytes=name_bytes,
                defaults=dict(
                    collection=email_file.collection,
                    ctime=email_file.ctime,
                    mtime=email_file.mtime,
                    size=size,
                    original=original,
                    blob=original,
                ),
            )

            handle_file.laterz(file.pk)
