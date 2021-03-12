"""Django model definitions file.

Also see `snoop.data.Collections` for details on how models are bound to the different databases.
"""

import string
from contextlib import contextmanager
from pathlib import Path
import tempfile
import hashlib
from django.db import models
from django.conf import settings
from django.template.defaultfilters import truncatechars
from django.db.models import JSONField
from django.core.exceptions import ObjectDoesNotExist
from .magic import Magic

from . import collections


def blob_root():
    """Returns a Path with the current collection blob dir.
    """
    col = collections.current()
    return Path(settings.SNOOP_BLOB_STORAGE) / col.name


def blob_repo_path(sha3_256):
    """Returns a Path pointing to the blob file for given hash.

    Args:
        sha3_256: hash used to compute the file path
    """
    return blob_root() / sha3_256[:2] / sha3_256[2:4] / sha3_256[4:]


def chunks(file, blocksize=65536):
    """Splits file into binary chunks of fixed size.

    Args:
        file: file-like object, already opened
        blocksize: size, in bytes, of the byte strings yielded
    """
    while True:
        data = file.read(blocksize)
        if not data:
            return
        yield data


class BlobWriter:
    """Compute binary blob size and hashes while also writing it in a file.
    """

    def __init__(self, file):
        """Constructor.

        Args:
            file: opened file, to write to
        """
        self.file = file
        self.hashes = {
            'md5': hashlib.md5(),
            'sha1': hashlib.sha1(),
            'sha3_256': hashlib.sha3_256(),
            'sha256': hashlib.sha256(),
        }
        self.size = 0

    def write(self, chunk):
        """Saves a byte string to file, while also updating size and hashes.

        Args:
            chunk: byte string to save to file
        """
        for h in self.hashes.values():
            h.update(chunk)
        self.file.write(chunk)
        self.size += len(chunk)

    def finish(self):
        """Return accumulated counters for size and hashes.

        Does not close file given to constructor.

        Returns:
            dict with fields 'size' and the various hashes
        """
        fields = {
            name: hash.hexdigest()
            for name, hash in self.hashes.items()
        }
        fields['size'] = self.size
        return fields


class Blob(models.Model):
    """Database model for storing binary objects, their hashes, and mime types.

    Every file that gets ingested by Hoover is cloned as a Blob and referenced
    in this table. Since the primary key is the hash of the data, all documents
    are de-duplicated.

    Intermediary results (like converted files, extracted files, JSON responses
    from other libraries and services, and the Digests, also in JSON) are also
    stored using this system, with no namespace separation. This means all our
    intermediary tasks tend to be de-duplicated too.

    Fields:
        sha3_256: hash (primary key)
        sha256: hash
        sha1: hash
        md5: hash

        size: in bytes
        magic: mime description given by libmagic
        mime_type: mime type given by libmagic
        mime_encoding: mime encoding given by libmagic

        date_created: auto-managed timestamp
        date_modified: auto-managed timestamp
    """

    sha3_256 = models.CharField(max_length=64, primary_key=True)
    sha256 = models.CharField(max_length=64, db_index=True)
    sha1 = models.CharField(max_length=40, db_index=True)
    md5 = models.CharField(max_length=32, db_index=True)

    size = models.BigIntegerField()
    magic = models.CharField(max_length=4096)
    mime_type = models.CharField(max_length=1024)
    mime_encoding = models.CharField(max_length=1024)

    date_created = models.DateTimeField(auto_now_add=True)
    date_modified = models.DateTimeField(auto_now=True)

    def __str__(self):
        """The string representation for a Blob is just its PK hash.
        """
        return self.pk

    __repr__ = __str__

    @property
    def content_type(self):
        """Returns a web-friendly content type string (for the HTTP header).
        """
        if self.mime_type.startswith('text/'):
            return f"{self.mime_type}; charset={self.mime_encoding}"

        return self.mime_type

    def path(self):
        """Returns a Path pointing to the disk location for this Blob.
        """
        return blob_repo_path(self.pk)

    @classmethod
    @contextmanager
    def create(cls, fs_path=None):
        """Context manager used for creating Blobs.

        Args:
            fs_path: optional filesystem path to file to get a more accurate
                reading for the mime type. If absent, the mime type will only
                be guessed from the data, without the help of the extension.
                Libmagic can't properly guess some vintage Microsoft formats
                without the extensions present.

        Yields:
            An instance of BlobWriter. Use `.write(byte_string)` on the
            returned object until finished. The final result can be found at
            `.blob` on the same object, after exiting this contextmanager's
            context.
        """
        blob_tmp = blob_root() / 'tmp'
        blob_tmp.mkdir(exist_ok=True, parents=True)

        fields = {}
        if fs_path:
            m = Magic(fs_path)
            fields = m.fields
        with tempfile.NamedTemporaryFile(dir=blob_tmp, delete=False) as f:
            writer = BlobWriter(f)
            yield writer

        fields.update(writer.finish())
        pk = fields.pop('sha3_256')

        blob_path = blob_repo_path(pk)
        blob_path.parent.mkdir(exist_ok=True, parents=True)
        temp_blob_path = Path(f.name)
        temp_blob_path.chmod(0o444)
        temp_blob_path.rename(blob_path)

        if not fs_path:
            m = Magic(blob_path)
            fields.update(m.fields)

        (blob, _) = cls.objects.get_or_create(pk=pk, defaults=fields)
        writer.blob = blob

    def _do_update_magic(self, path):
        """Update this object's magic fields by running libmagic on given path.

        Args:
            path: filesystem path used to recompute magic fields
        """
        f = Magic(path).fields
        self.mime_type = f['mime_type']
        self.mime_encoding = f['mime_encoding']
        self.magic = f['magic']
        self.save()

    def update_magic(self, path=None, filename=None):
        """Update magic fields for this object.

        Args:
            path: Optional filesystem Path. If exists, this is the best option.
            filename: Filename to be emulated when running libmagic. This
                option is needed when a filesystem location doesn't exist (for
                example, in an email).
        """
        if filename:
            # create temp dir;
            # create symlink to default path, with filename extension
            # run magic on symlink
            with tempfile.TemporaryDirectory() as d:
                filename = "File." + filename.split(b'.')[-1][:100].decode('utf-8', errors='surrogateescape')
                link_path = Path(d) / filename
                link_path.symlink_to(blob_repo_path(self.pk))
                self._do_update_magic(link_path)
                link_path.unlink()
                return

        if not path:
            path = blob_repo_path(self.pk)
        self._do_update_magic(path)

    @classmethod
    def create_from_bytes(cls, data):
        """Create a Blob from a single byte string.

        Useful when objects are in memory, for example when parsing email.

        Args:
            data: the byte string to be stored
        """
        sha3_256 = hashlib.sha3_256()
        sha3_256.update(data)

        try:
            b = Blob.objects.get(pk=sha3_256.hexdigest())
            return b

        except ObjectDoesNotExist:
            with cls.create() as writer:
                writer.write(data)
            return writer.blob

    @classmethod
    def create_from_file(cls, path):
        """Create a Blob from a file on disk.

        Since we know it has a stable path on disk, we take the luxury of
        reading it **twice**. We read it once to compute only the primary key
        hash, then close it, and if this is a new file, we reopen it and read
        the data.

        Args:
            path: string or Path to read from
        """
        path = Path(path).resolve().absolute()
        file_sha3_256 = hashlib.sha3_256()
        with open(path, 'rb') as f:
            for block in chunks(f):
                file_sha3_256.update(block)

        try:
            b = Blob.objects.get(pk=file_sha3_256.hexdigest())
            return b

        except ObjectDoesNotExist:
            with cls.create(path) as writer:
                with open(path, 'rb') as f:
                    for block in chunks(f):
                        writer.write(block)

            return writer.blob

    def open(self, encoding=None):
        """Open this Blob's data storage for reading.

        Args:
            encoding: if set, file is opened in text mode, and argument is used
            for string encoding. If not set, file is opened as binary.
        """
        if encoding is None:
            mode = 'rb'
        else:
            mode = 'r'
        return self.path().open(mode, encoding=encoding)


class Directory(models.Model):
    """Database model for a file directory.

    Along with File, this comprises the file tree structure analyzed by Hoover.
    A Directory can be found in two places: in anoter Directory, or as the only
    child of some archive or archive-like file.

    Fields:
        name_bytes: name of directory on disk, as bytes. We store this as bytes
            and not as strings because we have to support a multitude of original
            filesystems and encodings that create mutually invalid results.
        parent_directory: mutually exclusive with container_file
        container_file: mutually exclusive with parent_directory
    """

    name_bytes = models.BinaryField(max_length=1024, blank=True)
    parent_directory = models.ForeignKey(
        'Directory',
        null=True, blank=True,
        on_delete=models.CASCADE,
        related_name='child_directory_set',
    )
    container_file = models.ForeignKey(
        'File',
        null=True, blank=True,
        on_delete=models.CASCADE,
        related_name='child_directory_set',
    )

    date_created = models.DateTimeField(auto_now_add=True)
    date_modified = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ('parent_directory', 'name_bytes')
        verbose_name_plural = 'directories'

    @classmethod
    def root(cls):
        """Get the root of the whole filesystem.

        Raises:
            DoesNotExist if table empty.
        """
        return cls.objects.filter(
            parent_directory__isnull=True,
            container_file__isnull=True
        ).first()

    @property
    def name(self):
        """Decodes the name of this Directory as UTF-8.

        Escapes UTF-8 encoding errors with 'surrogateescape' - this has the
        advantage that it's reversible, for bad encodings.
        """
        name_bytes = self.name_bytes
        if isinstance(name_bytes, memoryview):
            name_bytes = name_bytes.tobytes()
        return name_bytes.decode('utf8', errors='surrogateescape')

    @property
    def parent(self):
        """Returns its parent, be it a File or Directory.
        """
        return self.parent_directory or self.container_file

    def ancestry(item):
        """Yields ancestors until root is found.
        """
        while item:
            yield item
            item = item.parent

    def __str__(self):
        """String representation for this Directory is its full path.
        """
        return ''.join(reversed([f'{item.name}/' for item in self.ancestry()]))

    __repr__ = __str__


class File(models.Model):
    """Database modle for a file found in the dataset.

    Fields:
        name_bytes: name of directory on disk, as bytes. We store this as bytes
            and not as strings because we have to support a multitude of original
            filesystems and encodings that create mutually invalid results.
        parent_directory: the directory containg this File
        ctime: taken from stat() or other sources
        mtime: taken from stat() or other sources
        size: size, taken from stat(), in bytes
        original: the original data found for this File
        blob: the converted data for this File. This is usually identical to
            `original`, but for some file formats conversion is required before
            any further processing (like apple email .emlx which is basically
            .eml with another some binary data prefixed to it).
    """

    name_bytes = models.BinaryField(max_length=1024, blank=True)
    parent_directory = models.ForeignKey(
        Directory,
        on_delete=models.CASCADE,
        related_name='child_file_set',
    )
    ctime = models.DateTimeField()
    mtime = models.DateTimeField()
    size = models.BigIntegerField()
    original = models.ForeignKey(Blob, on_delete=models.RESTRICT,
                                 related_name='+')
    blob = models.ForeignKey(Blob, on_delete=models.RESTRICT)

    date_created = models.DateTimeField(auto_now_add=True)
    date_modified = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ('parent_directory', 'name_bytes')

    @property
    def name(self):
        """Decodes the name of this Directory as UTF-8.

        Escapes UTF-8 encoding errors with 'surrogateescape' - this has the
        advantage that it's reversible, for bad encodings.
        """
        name_bytes = self.name_bytes
        if isinstance(name_bytes, memoryview):
            name_bytes = name_bytes.tobytes()
        return name_bytes.decode('utf8', errors='surrogateescape')

    def __str__(self):
        """String representation for a File is its filename, truncated.
        """
        return truncatechars(self.name, 80)

    __repr__ = __str__

    @property
    def parent(self):
        """parent.
        """
        return self.parent_directory


class Task(models.Model):
    """Database model for tracking status of the processing pipeline.

    Each row in this table tracks an application of a Python function to some
    arguments. Additional arguments can also be supplied as other Tasks that
    must run before this one.

    We keep track of the state of running the function:
    - "pending" (not run yet, OR started and not finished),
    - "deferred" (waiting on some other task to finish),
    - "success",
    - "error" (temporary error),
    - "broken" (permanent error)

    Fields:
        func: string key for Python function. See `snoop.data.tasks`.
        blob_arg: if the first argument is a Blob, it will be duplicated here.
        args: JSON containing arguments>
        result: a Blob with the result of running the function. Is set if
            finished successfully, and if the function actually returns a Blob
            value.
        date_started: timestamp when task started running. This isn't saved on
            the object when the task actually starts, in order to limit database
            writes.
        date_finished: timestamp when task has finished running, be it
            successfully or not.
        worker: identifier of the worker that finished this task. Not used.
        status: string token with task status; see above.
        error: text with stack trace, if status is "error" or "broken".
        broken_reason: identifier with reason for this permanent failure.
        log: text with complete log generated when this task was run.
    """

    STATUS_PENDING = 'pending'
    STATUS_SUCCESS = 'success'
    STATUS_ERROR = 'error'
    STATUS_DEFERRED = 'deferred'
    STATUS_BROKEN = 'broken'
    ALL_STATUS_CODES = [STATUS_PENDING, STATUS_BROKEN,
                        STATUS_DEFERRED, STATUS_ERROR, STATUS_SUCCESS]

    func = models.CharField(max_length=1024)
    blob_arg = models.ForeignKey(Blob, null=True, blank=True,
                                 on_delete=models.CASCADE,
                                 related_name='+')
    args = JSONField()
    result = models.ForeignKey(Blob, null=True, blank=True,
                               on_delete=models.RESTRICT)

    # these timestamps are used for logging and debugging, not for dispatching
    date_created = models.DateTimeField(auto_now_add=True)
    date_modified = models.DateTimeField(auto_now=True)
    date_started = models.DateTimeField(null=True, blank=True)
    # this timestamp is used for retrying errors
    date_finished = models.DateTimeField(null=True, blank=True)
    worker = models.CharField(max_length=4096, blank=True)

    status = models.CharField(max_length=16, default=STATUS_PENDING)
    error = models.TextField(blank=True)
    broken_reason = models.CharField(max_length=128, default='', blank=True)
    log = models.TextField(blank=True)

    class Meta:
        unique_together = ('func', 'args')
        indexes = [
            models.Index(fields=['status']),
            # stats for last minute
            models.Index(fields=['date_finished']),
            models.Index(fields=['func', 'status']),
            # for dispatching in reverse order
            models.Index(fields=['status', 'date_modified']),
            # for retrying all walks, in order
            models.Index(fields=['func', 'date_modified']),
        ]

    def __str__(self):
        """String representation for a Task contains its name, args and status.
        """
        deps = ''
        prev_set = self.prev_set.all()
        prev_ids = ', '.join(str(t.prev.pk) for t in prev_set)
        deps = '; depends on ' + prev_ids if prev_ids else ''
        return f'#{self.pk} {self.func}({self.args}{deps}) [{self.status}]'

    __repr__ = __str__

    def update(self, status, error, broken_reason, log):
        """Helper method to update multiple fields at once, without saving.

        This method also truncates our Text fields to decent limits, so it's
        preferred to use this instead of the fields directly.

        Args:
            status: field to set
            error: field to set
            broken_reason: field to set
            log: field to set
        """
        def _escape(s):
            """Escapes non-printable characters as \\XXX.

            Args:
                s: string to escape
            """
            def _translate(x):
                """Turns non-printable characters into \\XXX, prerves the rest.

                Args:
                    x:
                """
                if x in string.printable:
                    return x
                return f'\\{ord(x)}'
            return "".join(map(_translate, s))

        self.status = status
        self.error = _escape(error)[:2**13]  # 8k of error screen
        self.broken_reason = _escape(broken_reason)[:2**12]  # 4k reason
        self.log = _escape(log)[:2**14]  # 16k of log space


class TaskDependency(models.Model):
    """Database model for tracking which Tasks depend on which.

    Fields:
        prev: the task needed by another task
        next: the task taht depends on `prev`
        name: a string used to identify the kwarg name of this dependency
    """

    prev = models.ForeignKey(
        Task,
        on_delete=models.CASCADE,
        related_name='next_set',
    )
    next = models.ForeignKey(
        Task,
        on_delete=models.CASCADE,
        related_name='prev_set',
    )
    name = models.CharField(max_length=1024)

    class Meta:
        unique_together = ('prev', 'next', 'name')
        verbose_name_plural = 'task dependencies'

    def __str__(self):
        """String representation for a TaskDependency contains both task IDs
        and an arrow.
        """
        return f'{self.prev} -> {self.next}'

    __repr__ = __str__


class Digest(models.Model):
    """Digest contains all the data we have parsed for a de-duplicated
    document.

    The data is neatly stored as JSON in the "result" blob, ready for quick
    re-indexing if the need arises.

    Fields:
        blob: the de-duplicated Document for which processing has happened.
            This corresponds to `File.blob` not `File.original`.
        result: the Blob that contains the result of parsing the document,
            encoded as JSON. This may become huge, so we store it as a blob
            instead of a JSON field.
    """

    blob = models.OneToOneField(Blob, on_delete=models.CASCADE)
    result = models.ForeignKey(
        Blob,
        on_delete=models.RESTRICT,
        related_name='+',
    )

    date_created = models.DateTimeField(auto_now_add=True)
    date_modified = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=['date_modified']),
        ]

    def __str__(self):
        """To represent a Digest we use its blob hash and the result hash.
        """
        return f'{self.blob} -> {self.result.pk[:5]}...'

    __repr__ = __str__


class DocumentUserTag(models.Model):
    """Table used to store tags made by users.

    Both private and public tags are stored here.

    Private tags are stored on separate Elasticsearch fields, one field per
    user. Tags are referenced both by usernames and user UUIDs, since we can't
    use usernames as parts of the elasticsearch field name (since they can
    contain characters like dot '.' that cannot be part of a field name).

    Fields:
        digest: document being tagged
        user: username, as string (to send back in the API)
        uuid: unique identifier for user, used in elasticsearch field name
        tag: string with the actual tag
        public: boolean that decides type of tag
        date_indexed: moment when document containing this tag was re-indexed
    """

    digest = models.ForeignKey(Digest, on_delete=models.CASCADE)
    user = models.CharField(max_length=256)
    uuid = models.CharField(max_length=256, default="invalid")
    tag = models.CharField(max_length=512)
    public = models.BooleanField()
    date_created = models.DateTimeField(auto_now_add=True)
    date_modified = models.DateTimeField(auto_now=True)
    date_indexed = models.DateTimeField(null=True)

    class Meta:
        unique_together = ('digest', 'user', 'tag', 'public')
        indexes = [
            # in  digests.py _get_tag_timestamps
            models.Index(fields=['digest', 'tag', 'public', 'uuid', 'date_indexed']),
            models.Index(fields=['digest', 'public', 'tag', 'date_indexed']),
            # for paginating thru data
            models.Index(fields=['date_indexed']),
            models.Index(fields=['date_indexed', 'user', 'digest']),
        ]

    def __str__(self):
        return f'user tag {self.pk}: tag={self.tag} user={self.user} doc={self.blob.pk[:5]}...'

    @property
    def blob(self):
        """Returns the Blob containing the document for this tag.
        """

        return self.digest.blob

    @property
    def field(self):
        """Returns the elasticsearch field name for this tag.
        """

        # circular import
        from . import indexing

        if self.public:
            return indexing.PUBLIC_TAGS_FIELD_NAME
        return indexing.PRIVATE_TAGS_FIELD_NAME_PREFIX + self.uuid

    def save(self, *args, **kwargs):
        """Override for re-indexing document targeted by this tag.
        """

        self.date_indexed = None
        super().save(*args, **kwargs)

        from . import digests
        digests.retry_index(self.blob)

    def delete(self, *args, **kwargs):
        """Override for re-indexing document targeted by this tag.
        """

        super().delete(*args, **kwargs)

        from . import digests
        digests.retry_index(self.blob)


class OcrSource(models.Model):
    """Database model for a directory on disk containing External OCR files.


    Fields:
        name: identifier slug for this External OCR source. A directory called
            the same way must be present under the "ocr" directory in the
            collection location.
    """

    name = models.CharField(max_length=1024, unique=True)

    @property
    def root(self):
        """Returns the absolute path for the External OCR source.
        """
        col = collections.current()
        path = Path(settings.SNOOP_COLLECTION_ROOT) / col.name / 'ocr' / self.name
        assert path.is_dir()
        return path

    def __str__(self):
        return f"{self.pk}: {self.name}"

    __repr__ = __str__


class OcrDocument(models.Model):
    """Database model for External OCR result files found on disk.

    Fields:
        source: the OcrSource instance this document belongs to
        original_hash: the MD5 hash found on filesystem. The document targeted
            by this External OCR document is going to have the same MD5.
        ocr: a Blob with the data found (probably text or PDF)
        text: the extracted text for this entry (either read directly, or with
            pdftotext)
    """

    source = models.ForeignKey(OcrSource, on_delete=models.CASCADE)
    original_hash = models.CharField(max_length=64, db_index=True)
    ocr = models.ForeignKey(Blob, on_delete=models.RESTRICT)
    text = models.ForeignKey(Blob, on_delete=models.RESTRICT,
                             related_name='+')

    class Meta:
        unique_together = ('source', 'original_hash')


class Statistics(models.Model):
    """Database model for storing collection statistics.

    Most statistics queries take a long time to run, so we run them
    periodically (starting every few minutes, depending on server load).


    We store here things like task counts, % task progress status.

    Fields:
        key: string identifier for this statistic
        value: a JSON with computed result

    Scheduling is done separately, so there's no timestamps here.
    """

    key = models.CharField(max_length=64, unique=True)
    value = JSONField(default=dict)

    def __str__(self):
        return self.key

    class Meta:

        verbose_name_plural = 'statistics'
