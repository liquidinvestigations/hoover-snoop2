import json
import logging
from django.conf import settings

from snoop.profiler import profile

from . import digests
from . import models
from . import collections
from .analyzers import archives
from .analyzers import email
from .analyzers import emlx
from .tasks import snoop_task, require_dependency, SnoopTaskBroken
from .utils import time_from_unix
from .indexing import delete_doc

log = logging.getLogger(__name__)


def directory_absolute_path(directory):
    path_elements = []
    node = directory
    col = collections.current()
    path = col.data_path

    while node.parent_directory:
        path_elements.append(node.name)
        node = node.parent_directory
    for name in reversed(path_elements):
        path /= name

    return path


@snoop_task('filesystem.walk', priority=9)
@profile()
def walk(directory_pk):
    directory = models.Directory.objects.get(pk=directory_pk)
    path = directory_absolute_path(directory)

    for i, thing in enumerate(path.iterdir()):
        queue_limit = i >= settings.CHILD_QUEUE_LIMIT

        if thing.is_dir():
            (child_directory, created) = directory.child_directory_set.get_or_create(
                name_bytes=thing.name.encode('utf8', errors='surrogateescape'),
            )
            # since the periodic task retries all talk tasks in rotation,
            # we're not going to dispatch a walk task we didn't create
            walk.laterz(child_directory.pk, queue_now=created and not queue_limit)

        else:
            directory = models.Directory.objects.get(pk=directory_pk)
            path = directory_absolute_path(directory) / thing.name
            stat = path.stat()

            original = models.Blob.create_from_file(path)
            file, created = directory.child_file_set.get_or_create(
                name_bytes=thing.name.encode('utf8', errors='surrogateescape'),
                defaults=dict(
                    ctime=time_from_unix(stat.st_ctime),
                    mtime=time_from_unix(stat.st_mtime),
                    size=stat.st_size,
                    original=original,
                    blob=original,
                ),
            )
            # if file is already loaded, and size+mtime are the same,
            # don't retry handle task
            if created \
                    or file.mtime != time_from_unix(stat.st_mtime) \
                    or file.size != stat.st_size:
                file.mtime = time_from_unix(stat.st_mtime)
                file.size = stat.st_size
                file.original = original
                file.save()
                handle_file.laterz(file.pk, retry=True, queue_now=False)
            else:
                handle_file.laterz(file.pk, queue_now=False)


@snoop_task('filesystem.handle_file', priority=1)
@profile()
def handle_file(file_pk, **depends_on):
    file = models.File.objects.get(pk=file_pk)

    old_mime = file.original.mime_type
    old_blob_mime = file.blob.mime_type
    file.original.update_magic(filename=bytes(file.name_bytes))
    old_blob = file.blob
    file.blob = file.original

    # start choosing a conversion blob for this file
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
        except SnoopTaskBroken:
            pass

    elif file.original.mime_type == 'message/x-emlx':
        file.blob = require_dependency(
            'emlx_reconstruct', depends_on,
            lambda: emlx.reconstruct.laterz(file.pk),
        )

    if file.blob.pk != file.original.pk:
        file.blob.update_magic()

    if file.blob.mime_type == 'message/rfc822':
        email_parse_task = email.parse.laterz(file.blob)
        create_attachment_files.laterz(
            file.pk,
            depends_on={'email_parse': email_parse_task},
        )

    file.save()

    # if conversion blob changed from last time, then
    # we want to check if the old one is an orphan.
    deleted = False
    if file.blob.pk != old_blob.pk:
        if not old_blob.file_set.exists():
            # since it is an orphan, let's remove it from the index.
            log.warning('deleting orphaned blob from index: ' + old_blob.pk)
            delete_doc(old_blob.pk)
            deleted = True

    retry = file.original.mime_type != old_mime \
        or file.blob.mime_type != old_blob_mime \
        or deleted
    digests.launch.laterz(file.blob, retry=retry)


@snoop_task('filesystem.create_archive_files', priority=3)
@profile()
def create_archive_files(file_pk, archive_listing):
    if isinstance(archive_listing, SnoopTaskBroken):
        log.debug("Unarchive task is broken; returning without doing anything")
        return

    with archive_listing.open() as f:
        archive_listing_data = json.load(f)

    def create_directory_children(directory, children):
        for i, item in enumerate(children):
            queue_limit = i >= settings.CHILD_QUEUE_LIMIT

            if item['type'] == 'file':
                child_original = models.Blob.objects.get(pk=item['blob_pk'])
                file = create_file(directory, item['name'], child_original)
                handle_file.laterz(file.pk, queue_now=not queue_limit)

            if item['type'] == 'directory':
                create_directory(directory, item['name'], item['children'])

    def create_directory(parent_directory, name, children):
        (directory, _) = parent_directory.child_directory_set.get_or_create(
            name_bytes=name.encode('utf8', errors='surrogateescape'),
        )
        create_directory_children(directory, children)

    def create_file(parent_directory, name, original):
        size = original.path().stat().st_size

        file, _ = parent_directory.child_file_set.get_or_create(
            name_bytes=name.encode('utf8', errors='surrogateescape'),
            defaults=dict(
                ctime=archive.ctime,
                mtime=archive.mtime,
                size=size,
                original=original,
                blob=original,
            ),
        )

        return file

    archive = models.File.objects.get(pk=file_pk)
    (fake_root, _) = archive.child_directory_set.get_or_create(name_bytes=b'')
    create_directory_children(fake_root, archive_listing_data)


def get_email_attachments(parsed_email):
    if isinstance(parsed_email, SnoopTaskBroken):
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


@snoop_task('filesystem.create_attachment_files', priority=2)
@profile()
def create_attachment_files(file_pk, email_parse):
    attachments = list(get_email_attachments(email_parse))

    if attachments:
        email_file = models.File.objects.get(pk=file_pk)
        (attachments_dir, _) = email_file.child_directory_set.get_or_create(
            name_bytes=b'',
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
                    ctime=email_file.ctime,
                    mtime=email_file.mtime,
                    size=size,
                    original=original,
                    blob=original,
                ),
            )

            handle_file.laterz(file.pk)
