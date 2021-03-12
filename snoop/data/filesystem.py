"""Definitions for filesystem-related Tasks.

These are the first steps in processing any dataset: walking the filesystem and recording files and
directories in their respective database tables.

The root directory for a dataset is explored by the `walk()` function. This recursively runs `walk()` on the
directories inside it, and then runs `handle_file()` on all the files inside it.

Files discovered may actually be Archives, Emails or other containers that contain directories too, but
walking these is treated differently under `snoop.data.analyzers.archives`, `snoop.data.analyzers.email` and
others.
"""

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
from .tasks import snoop_task, require_dependency, remove_dependency, SnoopTaskBroken
from .utils import time_from_unix
from .indexing import delete_doc

log = logging.getLogger(__name__)


def directory_absolute_path(directory):
    """Returns absolute Path for a dataset Directory.

    This is expected to return an invalid Path without failing if supplied with a Directory that is not
    found directly on the filesystem, but inside some archive or email.
    """
    # TODO: throw exception if directory isn't directly on filesystem

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
    """Scans one level of a directory and recursively ingests all files and directories found.

    Items are iterated in arbitrary order.  When a directory was found, an entry is added to the `Directory`
    table (if it doesn't exist there already) and another `walk()` task is queued on it. On the other hand,
    if a file was fonud, a corresponding row is added (or updated, but never removed) from the `File` table,
    the binary data for the file is stored in a Blob object, and finally the `handle_file()` task is queued
    for it.

    One of
    the decorators of this function, `snoop.data.tasks.snoop_task()`, wraps this function in a Django
    Transaction. Because `snoop.data.tasks.queue_task()` also wraps the queueing operation inside Django's
    `transaction.on_commit()`, all queueing operations will be handled after the transaction (and function)
    is finished successfully. This has two effects: we never queue tasks triggered from failing functions
    (to avoid cascading errors), and we queue all the next tasks after finishing running the current task.

    The number of tasks queued by this one is limited to a config setting called `CHILD_QUEUE_LIMIT`. Since
    the tasks to be queued are kept in memory until the function is finished, we are limiting the memory
    used through this value. We also want to avoid saturating the message queue with writes, a real
    I/O bottleneck.

    For the `walk() -> handle_file()` queueing, this is the intended behavior, because `handle_file`
    requires all sibling Files to have also been saved in the File table and on the Blobs. This is needed
    because some file types depend on their sibling files to be decoded. For example, the Apple Email format
    (".emlx") stores larger parts of its multipart RFC-822 messages in separate files under the same
    Directory. If we would run `handle_file()` on any file without its siblings existing in the database, we
    woulnd't find those attachment files.

    For the `walk() -> walk()` queueing, this is not the intended behavior - we could save some time in the
    beginning of processing a dataset by dispatching all the `walk()` faster, saturating the workers
    quicker. We take a conservative approach here in case we wanted to add deeper matching of related files
    (for example, what if the ".emlx" files stored their attachment under a sub-folder?). Since this
    functionality is not used, the `walk() -> walk()` recursivity may be optimized in the future by removing
    the `transaction.on_commit` call from `queue_task()` when queueing `walk()` from this function.
    """
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
                handle_file.laterz(file.pk, retry=True, queue_now=not queue_limit)
            else:
                handle_file.laterz(file.pk, queue_now=False)


@snoop_task('filesystem.handle_file', priority=1)
@profile()
def handle_file(file_pk, **depends_on):
    """Parse, update and possibly convert file found on in dataset.

    Re-runs `libmagic` in case mime type changed (through updating the library). Switching by the resulting
    mime type, a decision is made if the file needs to be converted to another format, or unpacked into more
    Files and Directories (in cases like archives, emails with attachments, PDFs with images, etc).

    If a conversion/unpacking is required, then a Task responsible for doing the conversion/unpacking
    operation is dynamically added as a dependency for this Task (using `require_dependency()`). Previous
    dependencies that are not valid anymore must also be removed here; this is to fix documents with wront
    mime types, not to support document deletion.

    Finally, after all unarchiving, unpacking and converting is done, we queue the `digests.launch` Task for
    the de-duplicated document that the given File is pointing to.  A dependency between this Task and that
    one is not made, since we have no use for such a dependency and it would only slow down the database.
    """
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

    if file.original.mime_type in email.OUTLOOK_POSSIBLE_MIME_TYPES:
        try:
            file.blob = require_dependency(
                'msg_to_eml', depends_on,
                lambda: email.msg_to_eml.laterz(file.original),
            )
        except SnoopTaskBroken:
            pass
    else:
        remove_dependency('msg_to_eml', depends_on)

    if file.original.mime_type == 'message/x-emlx':
        file.blob = require_dependency(
            'emlx_reconstruct', depends_on,
            lambda: emlx.reconstruct.laterz(file.pk),
        )
    else:
        remove_dependency('emlx_reconstruct', depends_on)

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
    if file.blob.pk != old_blob.pk and old_blob.pk != file.original.pk:
        if not old_blob.file_set.exists():
            # since it is an orphan, let's remove it from the index
            log.warning('deleting orphaned blob from index: ' + old_blob.pk)
            delete_doc(old_blob.pk)

            # and database - this should cascade into all tasks, Digests, etc
            old_blob.delete()

            deleted = True

    retry = file.original.mime_type != old_mime \
        or file.blob.mime_type != old_blob_mime \
        or deleted
    digests.launch.laterz(file.blob, retry=retry)


@snoop_task('filesystem.create_archive_files', priority=3)
@profile()
def create_archive_files(file_pk, archive_listing):
    """Creates the File and Directoty structure after unpacking files.

    Receives a dict (called the "archive_listing") from the `unarchive()` task with the names of the
    Files and Directories that must be created, as well as the File timestmaps and binary data hashes.

    This function serves the role of `walk()`, but inside archives; it queues `handle_file()` for all
    files unpacked.
    """

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
    """Extracts attachment identifiers from parsed email data.
    """

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
    """Creates the File and Directoty structure after unpacking email attachments.

    Receives a dict from the `email.parse()` task with the names and bodies of the attachments.

    This function serves the role of `walk()`, but inside emails; it queues `handle_file()` for all
    files unpacked.
    """

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
