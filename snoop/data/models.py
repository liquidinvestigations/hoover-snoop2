"""Django model definitions file.

Also see [snoop.data.collections][] for details on how models are bound to the different
databases.
"""

import hashlib
import os
import string
import json
from contextlib import contextmanager
from pathlib import Path
import tempfile
import logging
import operator
import functools

from django.db import models
from django.conf import settings
from django.template.defaultfilters import truncatechars
from django.db.models import JSONField, Q
from django.core.exceptions import ObjectDoesNotExist, ValidationError
from smart_open import open as smart_open

from psqlextra.types import PostgresPartitioningMethod
from psqlextra.models import PostgresPartitionedModel
from psqlextra.indexes import UniqueIndex

from .magic import Magic

from . import collections


logger = logging.getLogger(__name__)


def blob_repo_path(sha3_256):
    """Returns a string pointing to the blob object for given hash.

    Args:
        sha3_256: hash used to compute the object path
    """
    return sha3_256[:2] + '/' + sha3_256[2:4] + '/' + sha3_256[4:]


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

    def __init__(self, file=None):
        """Constructor.

        Args:
            file: opened file, to write to, optional.
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
        if self.file:
            self.file.write(chunk)
        self.size += len(chunk)

    def finish(self):
        """Return accumulated counters for size and hashes.

        Does not close file given to constructor.

        Returns:
            dict: with fields 'size' and the various hashes
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
    """

    sha3_256 = models.CharField(max_length=64, primary_key=True)
    """hash of content (primary key)"""

    sha256 = models.CharField(max_length=64, db_index=True)
    """hash of content"""

    sha1 = models.CharField(max_length=40, db_index=True)
    """hash of content"""

    md5 = models.CharField(max_length=32, db_index=True)
    """hash of content"""

    size = models.BigIntegerField(db_index=True)
    """size of content, bytes."""

    magic = models.CharField(max_length=4096)
    """mime description given by libmagic (`man 1 file`)."""

    mime_type = models.CharField(max_length=1024)
    """mime type given by libmagic."""

    mime_encoding = models.CharField(max_length=1024)
    """mime encoding given by libmagic, for text files."""

    collection_source_key = models.BinaryField(max_length=4096, blank=True)
    """If this is set, we store and retrieve the file using this key from the collections S3 instead of the
    default blobs S3."""

    # archive_source_key = models.BinaryField(max_length=4096, blank=True)
    # """[DEPRECATED] Old key for archive mounting. Can't delete it yet."""

    # archive_source_blob = models.ForeignKey(
    #     'Blob',
    #     null=True,
    #     on_delete=models.RESTRICT,
    #     related_name='archive_children_blobs',
    # )
    # """[DEPRECATED] Old key for archive mounting. Can't delete it yet"""

    date_created = models.DateTimeField(auto_now_add=True)
    """Auto-managed timestamp."""

    date_modified = models.DateTimeField(auto_now=True)
    """Auto-managed timestamp."""

    def __str__(self):
        """The string representation for a Blob is just its PK hash.
        """
        the_str = truncatechars(self.pk, 10)
        return f'Blob({the_str})'

    __repr__ = __str__

    @property
    def content_type(self):
        """Returns a web-friendly content type string (for the HTTP header).
        """
        if self.mime_type.startswith('text/'):
            return f"{self.mime_type}; charset={self.mime_encoding}"

        return self.mime_type

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
            [snoop.data.models.BlobWriter][] -- Use `.write(byte_string)` on the returned object until
            finished. The final result can be found at `.blob` on the same object, after exiting this
            contextmanager's context.
        """

        fields = {}
        if fs_path:
            m = Magic(fs_path)
            fields = m.fields
        with tempfile.NamedTemporaryFile(prefix='new-blob-', delete=False) as f:
            writer = BlobWriter(f)
            yield writer

        fields.update(writer.finish())
        pk = fields.pop('sha3_256')

        temp_blob_path = Path(f.name)
        temp_blob_path.chmod(0o444)

        if not fs_path:
            m = Magic(temp_blob_path)
            fields.update(m.fields)

        settings.BLOBS_S3.fput_object(
            collections.current().name,
            blob_repo_path(pk),
            temp_blob_path,
        )

        (blob, _) = cls.objects.get_or_create(pk=pk, defaults=fields)
        writer.blob = blob

        os.remove(temp_blob_path)

    def update_magic(self):
        """Refreshes the mime type fields by running libmagic on the mounted blob.

        Updates the database object if needed.
        """
        with self.mount_path() as blob_path:
            m = Magic(Path(blob_path))
            fields = m.fields
        changed = False
        for k, v in fields.items():
            if v != getattr(self, k):
                setattr(self, k, v)
                changed = True
        if changed:
            self.save()

    @property
    def repo_path(self):
        return blob_repo_path(self.pk)

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
    def create_json(cls, data):
        """Create a Blob containing JSON encoded data from the given Python dict object."""
        return cls.create_from_bytes(json.dumps(data, indent=1).encode('utf-8'))

    @classmethod
    def create_from_file(cls, path, collection_source_key=None):
        """Create a Blob from a file on disk.

        Since we know it has a stable path on disk, we take the luxury of
        reading it **twice**. We read it once to compute only the primary key
        hash, then close it, and if this is a new file, we reopen it and read
        the data.

        Args:
            path: string or Path to read from.
            collection_source_key: if set, will use the collection source bucket as storage.
        """
        path = Path(path).resolve().absolute()
        writer = BlobWriter()
        with open(path, 'rb') as f:
            for block in chunks(f):
                writer.write(block)
        fields = writer.finish()
        pk = fields.pop('sha3_256')

        try:
            b = Blob.objects.get(pk=pk)
            if collection_source_key and not b.collection_source_key:
                # delete this from minio and override/save new key
                try:
                    settings.BLOBS_S3.remove_object(collections.current().name, blob_repo_path(b.pk))
                    logger.info('successfully deleted object from s3.')
                except Exception as e:
                    logger.exception(e)
                    logger.error('failed to delete object from s3.')

                b.collection_source_key = collection_source_key
                b.save()
                return b

            # ensure the S3 object still exists by checking it
            try:
                stat = settings.BLOBS_S3.stat_object(
                    collections.current().name,
                    blob_repo_path(pk),
                )
                assert stat is not None, 'empty stat'
            except Exception as e:
                logger.warning('error getting stat (%s); re-uploading blob %s...', str(e), pk)
                settings.BLOBS_S3.fput_object(
                    collections.current().name,
                    blob_repo_path(pk),
                    path,
                )
            return b

        except ObjectDoesNotExist:
            if collection_source_key:
                m = Magic(path)
                fields.update(m.fields)
                fields['collection_source_key'] = collection_source_key
                (blob, _) = cls.objects.get_or_create(pk=pk, defaults=fields)
                return blob

            with cls.create(path) as writer:
                with open(path, 'rb') as f:
                    for block in chunks(f):
                        writer.write(block)

            return writer.blob

    @contextmanager
    def mount_path(self):
        """Mount this blob under some temporary directory using s3fs-fuse / fuse-7z-ng and return its
        path."""

        if self.collection_source_key:
            with collections.current().mount_collections_root() as collection_root:
                key_str = self.collection_source_key.tobytes().decode('utf-8', errors='surrogateescape')
                yield os.path.join(collection_root,
                                   key_str)

        else:
            with collections.current().mount_blobs_root() as blobs_root:
                key = blob_repo_path(self.pk)
                yield os.path.join(blobs_root, key)

    @contextmanager
    def open(self, need_seek=False, need_fileno=False):
        """Open this Blob's data storage for reading. Mode is always 'rb'.

        Args:
            - need_seek: if the returned file object requires `f.seek()`, for example with Python libraries.
                If this is the only flag set, this is achieved by using the `smart_open` library.
            - need_fileno: if the returned file object requires `f.fileno()`, for example with `subprocess`
                calls where this is given as standard input. If this is the only flag set, this is achieved
                by making a local FIFO pipe (`os.mkfifo` and pushing data into that, from a forked process).

        If both arguments are set to `true`, then we use `mount_path()` to get a FUSE filesystem containing
        the files, and return the file object by opening that path.

        Some programs don't even accept any kind of input from stdin, such as `7z` with most formats, or
        `pdf2pdfocr.py`, which just exits (probably knowing it'll do multiple seek and multiple opens).

        In that case, just use the `mount_path` contextmanager to get a POSIX filesystem path.
        """
        # if (need_seek and need_fileno):
        if (need_fileno):
            with self.mount_path() as blob_path:
                yield open(blob_path, mode='rb')
                return

        if self.collection_source_key:
            bucket = collections.current().name
            key = self.collection_source_key.tobytes().decode('utf-8', errors='surrogateescape')
            smart_transport_params = settings.SNOOP_COLLECTIONS_SMART_OPEN_TRANSPORT_PARAMS
            minio_client = settings.COLLECTIONS_S3
        else:
            bucket = collections.current().name
            key = blob_repo_path(self.pk)
            smart_transport_params = settings.SNOOP_BLOBS_SMART_OPEN_TRANSPORT_PARAMS
            minio_client = settings.BLOBS_S3

        if need_seek:
            url = f's3u://{bucket}/{key}'
            yield smart_open(
                url,
                transport_params=smart_transport_params,
                mode='rb',
            )
            return

        # This works on subprocess calls, **but** if the process fails, they hang forever.
        # TODO We need to find an alternative to this, that works good when the process fails.
        # elif need_fileno:
        #     # Supply opened unix pipe. Pipe is written to by fork.
        #     with tempfile.TemporaryDirectory(prefix=f'blob-fifo-{self.pk}-') as d:
        #         fifo = os.path.join(d, 'fifo')
        #         os.mkfifo(fifo, 0o600)
        #         if os.fork() > 0:
        #             logger.info('parent process: call open on fifo')
        #             yield open(fifo, mode='rb')
        #         else:
        #             logger.info('child process: write into fifo')
        #             r = None
        #             try:
        #                 r = minio_client.get_object(bucket, key)
        #                 with open(fifo, mode='wb') as fwrite:
        #                     while (b := r.read(2 ** 20)):
        #                         fwrite.write(b)
        #             finally:
        #                 if r:
        #                     r.close()
        #                     r.release_conn()
        #                 logger.info('child process: exit')
        #                 os._exit(0)
        else:
            r = None
            try:
                r = minio_client.get_object(bucket, key)
                yield r
            finally:
                if r:
                    r.close()
                    r.release_conn()

    def read_json(self):
        """Load a JSON encoded binary into a python dict in memory.
        """
        with self.open() as f:
            return json.load(f)


class Directory(models.Model):
    """Database model for a file directory.

    Along with [File][snoop.data.models.File], this comprises the file tree structure analyzed by Hoover. A
    Directory can be found in two places: in another Directory, or as the only child of some archive or
    archive-like file.
    """

    name_bytes = models.BinaryField(max_length=1024, blank=True)
    """Name of directory on disk, as bytes.

    We store this as bytes and not as strings because we have to support a multitude of original filesystems
    and encodings that create mutually invalid results.
    """

    parent_directory = models.ForeignKey(
        'Directory',
        null=True, blank=True,
        on_delete=models.CASCADE,
        related_name='child_directory_set',
    )
    """The parent, if it is a directory, or NULL.

    Mutually exclusive with [snoop.data.models.Directory.container_file][].
    """

    container_file = models.ForeignKey(
        'File',
        null=True, blank=True,
        on_delete=models.CASCADE,
        related_name='child_directory_set',
    )
    """The parent, if it's a file (archive, email-archive or something else), else NULL.

    Mutually exclusive with [snoop.data.models.Directory.parent_directory][].
    """

    date_created = models.DateTimeField(auto_now_add=True)
    date_modified = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ('parent_directory', 'name_bytes')
        verbose_name_plural = 'directories'

    @classmethod
    def root(cls):
        """Get the root of the whole filesystem.

        Raises:
            DoesNotExist: if table empty.
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

    @property
    def path_str(self):
        """Returns a string representation of its full path."""
        return ''.join(reversed([f'{item.name}/' for item in self.ancestry()]))

    def __str__(self):
        """String representation for this Directory is its full path.
        """
        # ensure no display errors by replacing surrogates with backslashes
        name = self.path_str.encode('utf8', errors='surrogateescape')
        name = name.decode('utf8', errors='backslashreplace')
        return truncatechars(name, 70)

    __repr__ = __str__


class File(models.Model):
    """Database modle for a file found in the dataset.
    """

    name_bytes = models.BinaryField(max_length=1024, blank=True)
    """Name of file on disk, as bytes.

    We store this as bytes and not as strings because we have to support a multitude of original filesystems
    and encodings that create mutually invalid results.
    """

    parent_directory = models.ForeignKey(
        Directory,
        on_delete=models.CASCADE,
        related_name='child_file_set',
    )
    """The directory containg this File.
    """

    ctime = models.DateTimeField()
    """Taken from stat() or other sources.
    """

    mtime = models.DateTimeField()
    """Taken from stat() or other sources.
    """

    size = models.BigIntegerField(db_index=True)
    """Size, taken from stat(), in bytes.
    """

    original = models.ForeignKey(Blob, on_delete=models.RESTRICT,
                                 related_name='original_file_set')
    """The original data found for this File.
    """

    blob = models.ForeignKey(Blob, on_delete=models.RESTRICT)
    """The converted data for this File.

    This is usually identical to `original`, but for some file formats conversion is required before any
    further processing (like apple email .emlx which is basically .eml with another some binary data
    prefixed to it).
    """

    date_created = models.DateTimeField(auto_now_add=True)
    date_modified = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ('parent_directory', 'name_bytes')

    @property
    def name(self):
        """Decodes the name of this File as UTF-8.

        Escapes UTF-8 encoding errors with 'surrogateescape' - this has the
        advantage that it's reversible, for bad encodings.
        """
        name_bytes = self.name_bytes
        if isinstance(name_bytes, memoryview):
            name_bytes = name_bytes.tobytes()
        return name_bytes.decode('utf8', errors='surrogateescape')

    def __str__(self):
        """String representation for a File is its filename,
        with non-UTF8 code points escaped with backslashes, truncated.
        """
        name_bytes = self.name_bytes
        if isinstance(name_bytes, memoryview):
            name_bytes = name_bytes.tobytes()
        the_str = truncatechars(name_bytes.decode('utf8', errors='backslashreplace'), 60)
        return f'File({the_str})'

    __repr__ = __str__

    @property
    def parent(self):
        """Returns the ID of the parent directory.
        """
        return self.parent_directory


class Digest(models.Model):
    """Digest contains all the data we have parsed for a de-duplicated
    document.

    The data is neatly stored as JSON in the "result" blob, ready for quick
    re-indexing if the need arises.
    """

    blob = models.OneToOneField(Blob, on_delete=models.CASCADE)
    """The de-duplicated Document for which processing has happened.

    This corresponds to [snoop.data.models.File.blob][], not [snoop.data.models.File.original][].
    """

    result = models.ForeignKey(
        Blob,
        null=True,
        on_delete=models.RESTRICT,
        related_name='digest_result_set',
    )
    """The Blob that contains the result of parsing the document, encoded as JSON.

    This output is generated by the `digests.gather` task.

    This may become huge, so we store it as a Blob instead of a JSON field.
    """

    extra_result = models.ForeignKey(
        Blob,
        null=True,
        on_delete=models.RESTRICT,
        related_name='digest_extra_result_set',
    )
    """The Blob that contains the result of the `digests.index` task, encoded as JSON.
    The field is optional, and required by tasks that depend on the `

    This may become huge, so we store it as a Blob instead of a JSON field.
    """

    date_created = models.DateTimeField(auto_now_add=True)
    date_modified = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=['date_modified']),
            models.Index(fields=['date_created']),
        ]

    def __str__(self):
        """To represent a Digest we use its blob hash and the result hash.
        """
        return f'{self.blob} -> {self.result.pk[:5]}...'

    def get_etag(self):
        """Compute HTTP ETag header for this Digest.
        To be used for implementing caching mechanisms."""
        etag = str(self.pk)
        etag += ':'
        if self.result:
            etag += str(self.result.pk)
        etag += ':'
        if self.extra_result:
            etag += str(self.extra_result.pk)
        etag += ':'
        etag += str(self.date_modified)
        etag += ':'
        etag += str(self.date_created)
        etag = etag.encode('utf-8', errors='backslashreplace')
        etag = hashlib.sha1(etag).hexdigest()
        return etag

    __repr__ = __str__


class DocumentUserTag(models.Model):
    """Table used to store tags made by users.

    Both private and public tags are stored here.

    Private tags are stored on separate Elasticsearch fields, one field per
    user. Tags are referenced both by usernames and user UUIDs, since we can't
    use usernames as parts of the elasticsearch field name (since they can
    contain characters like dot '.' that cannot be part of a field name).
    """

    digest = models.ForeignKey(Digest, on_delete=models.CASCADE)
    """Document being tagged.
    """

    user = models.CharField(max_length=256)
    """Username, as string (to send back in the API).

    """

    uuid = models.CharField(max_length=256, default="invalid")
    """Unique identifier for user, used in elasticsearch field name.
    """

    tag = models.CharField(max_length=512)
    """String with the actual tag.
    """

    public = models.BooleanField()
    """Boolean that decides type of tag
    """

    date_created = models.DateTimeField(auto_now_add=True)
    date_modified = models.DateTimeField(auto_now=True)

    date_indexed = models.DateTimeField(null=True)
    """Moment when document containing this tag was re-indexed.
    """

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

    def check_tag_name(self):
        if any(char in self.tag for char in string.whitespace):
            raise ValidationError('tag name invalid')

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

        self.check_tag_name()

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


class EntityType(models.Model):
    """ Database model for an entity type. Per data migration, the following are added
    automatically.
    """
    type = models.CharField(max_length=256, unique=True)

    def __str__(self):
        return f'{self.type}'

    __repr__ = __str__


class Entity(models.Model):
    """ Database model for Entities. Entities have a textfield for their string
    and a type. Additionally, they may have a parent (if merged), or can be
    blacklisted (so not shown as entities).
    """
    entity = models.CharField(max_length=1024)
    type = models.ForeignKey(EntityType, on_delete=models.CASCADE)
    parent = models.ForeignKey('self', blank=True, null=True,
                               related_name='children', on_delete=models.CASCADE)
    blacklisted = models.BooleanField(default=False)

    class Meta:
        unique_together = ('entity', 'type')

    def __str__(self):
        return f'entity.{self.type.type}: {self.entity}'

    __repr__ = __str__


class LanguageModel(models.Model):
    """ Database model for language models. This can be  used to filter for
    specific results of language models.
    The language code is the language code of the used language, or 'mlt' for
    multilingual models. The engine is either `spacy` or `polyglot`.
    The description is the string of the model, for example 'xx_ent_wiki_sm'
    for the multilingual spacy model which is based on the WikiNER data set.
    """
    language_code = models.CharField(max_length=3)
    engine = models.CharField(max_length=256)
    model_name = models.CharField(max_length=256, unique=True)

    class Meta:
        unique_together = ('language_code', 'engine', 'model_name')

    def __str__(self):
        return f'{self.model_name}'

    __repr__ = __str__


class EntityHit(models.Model):
    """ Database model for an entitiy hit.
    An entity hit is a hit of an entitiy in a text source, which means that the
    entity was found in the text (more specific between index start and end).
    The used language model is also stored as a foreign key in order to discern which
    language model produced the hit.
    """
    entity = models.ForeignKey(Entity, on_delete=models.CASCADE)
    digest = models.ForeignKey(Digest, on_delete=models.CASCADE)
    model = models.ForeignKey(LanguageModel, on_delete=models.CASCADE)
    text_source = models.CharField(max_length=256)
    start = models.PositiveIntegerField()
    end = models.PositiveIntegerField()

    def __str__(self):
        return f'{self.entity}'

    __repr__ = __str__


class OcrSource(models.Model):
    """Database model for a directory on disk containing External OCR files.
    """

    name = models.CharField(max_length=1024, unique=True)
    """Identifier slug for this External OCR source

    A directory called the same way must be present under the "ocr" directory in the collection location.
    """

    @contextmanager
    def mount_root(self):
        """Returns the absolute path for the External OCR source.
        """

        with collections.current().mount_collections_root() as collection_root:
            path = Path(collection_root) / 'ocr' / self.name
            assert path.is_dir()
            yield path

    def __str__(self):
        return f"{self.pk}: {self.name}"

    __repr__ = __str__


class OcrDocument(models.Model):
    """Database model for External OCR result files found on disk."""

    source = models.ForeignKey(OcrSource, on_delete=models.CASCADE)
    """The OcrSource instance this document belongs to."""

    original_hash = models.CharField(max_length=64, db_index=True)
    """The MD5 hash found on filesystem.

    The document targeted by this External OCR document is going to
    have the same MD5.
    """

    ocr = models.ForeignKey(Blob, on_delete=models.RESTRICT)
    """A Blob with the data found (probably text or PDF).
    """

    text = models.ForeignKey(Blob, on_delete=models.RESTRICT,
                             related_name='ocr_document_text_set')
    """The extracted text for this entry (either read directly, or with pdftotext).
    """

    class Meta:
        unique_together = ('source', 'original_hash')


class Statistics(models.Model):
    """Database model for storing collection statistics.

    Most statistics queries take a long time to run, so we run them
    periodically (starting every few minutes, depending on server load).


    We store here things like task counts, % task progress status.

    Scheduling is done separately, so there's no timestamps here.
    """

    key = models.CharField(max_length=64, unique=True)
    """string identifier for this statistic. """

    value = JSONField(default=dict)
    """JSON with computed result."""

    def __str__(self):
        return str(self.key)

    class Meta:

        verbose_name_plural = 'statistics'


class Thumbnail(models.Model):
    """Database model for storing the Thumbnail corresponding to a Digest.
    """
    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['blob', 'size'], name='unique_size')
        ]

    class SizeChoices(models.IntegerChoices):
        SMALL = 100
        MEDIUM = 200
        LARGE = 400

    source = models.ForeignKey(
        Blob,
        null=True,
        on_delete=models.CASCADE,
        related_name='thumbnail_source_set',
    )
    """Foreign Key to the blob used for computation."""

    blob = models.ForeignKey(
        Blob,
        on_delete=models.CASCADE,
        related_name='thumbnail_original_set',
    )
    """Foreign Key to the original File's blob"""

    thumbnail = models.ForeignKey(
        Blob,
        on_delete=models.RESTRICT,
        related_name='thumbnail_result_set',
    )
    """Foreign Key to the corresponding thumbnail-blob."""

    size = models.IntegerField(choices=SizeChoices.choices, default=SizeChoices.MEDIUM)


class PdfPreview(models.Model):
    """Database model for storing the pdf preview corresponding to a document.
    """

    blob = models.ForeignKey(
        Blob,
        on_delete=models.CASCADE,
        related_name='pdf_preview_original_set'
    )

    pdf_preview = models.ForeignKey(
        Blob,
        on_delete=models.RESTRICT,
        related_name='pdf_preview_result_set'
    )


class Task(PostgresPartitionedModel):
    """Database model for tracking status of the processing pipeline.

    Each row in this table tracks an application of a Python function to some
    arguments. Additional arguments can also be supplied as other Tasks that
    must run before this one.
    """

    STATUS_PENDING = 'pending'
    """Task either wasn't run yet, or was started but not finished.

    Making the difference between `pending` and `running` requires a write to happen inside our transaction,
    so we can't tell from outside the runner anyway.
    """

    STATUS_SUCCESS = 'success'
    """Task finished successfully."""

    STATUS_ERROR = 'error'
    """Unexpected error.

    Might be termporary, might be permanent, we don't know.
    """

    STATUS_STARTED = 'started'
    """Has been started by the worker at some point.

    Used to detect when Python process was unexpectedly Killed, e.g. from OOM."""

    STATUS_QUEUED = 'queued'
    """Used for tasks that have been put on the queue."""

    STATUS_DEFERRED = 'deferred'
    """Waiting on some other task to finish."""

    STATUS_BROKEN = 'broken'
    """Permanent error.

    Used to some known type of breakage, such as: encrypted archives, encrypted PDFs, or if dependencies are
    in an ERROR state too."""

    ALL_STATUS_CODES = [STATUS_PENDING, STATUS_BROKEN,
                        STATUS_DEFERRED, STATUS_ERROR, STATUS_SUCCESS, STATUS_STARTED, STATUS_QUEUED]
    """List of all valid status codes.

    TODO:
        We should really change these out for Enums at some point.
    """

    func = models.CharField(max_length=1024)
    """ String key for Python function.

    Supplied as argument in the decorator [snoop.data.tasks.snoop_task][].

    See [snoop.data.tasks][] for general definition and [snoop.data.filesystem][],
    [snoop.data.analyzers.__init__][] and [snoop.data.digests][] for actual Task implementations.
    """

    blob_arg = models.ForeignKey(Blob, null=True, blank=True,
                                 on_delete=models.CASCADE,
                                 related_name='task_arg_set')
    """ If the first argument is a Blob, it will be duplicated here.

    Used to optimize fetching tasks, as most tasks will only process one Blob as input.
    """

    args = JSONField()
    """JSON containing arguments.
    """

    result = models.ForeignKey(Blob, null=True, blank=True,
                               on_delete=models.RESTRICT)
    """
    Binary object with result of running the function.

    Is set if finished successfully, and if the function actually returns a Blob value.
    """

    # these timestamps are used for logging and debugging, not for dispatching
    # so don't index them!
    date_created = models.DateTimeField(auto_now_add=True)
    date_modified = models.DateTimeField(auto_now=True)

    date_started = models.DateTimeField(null=True, blank=True)
    """Moment when task started running.

    This isn't saved on the object when the task actually starts, in order to limit database writes.
    """

    date_finished = models.DateTimeField(null=True, blank=True)
    """Moment when task finished running.

    Used in logic for retrying old errors and re-running sync tasks.
    """

    version = models.IntegerField(default=0)
    """The version of the function that ran this task.

    Used to re-process data when the code (version number) is changed.
    """

    fail_count = models.IntegerField(default=0)
    """The number of times this function has failed in a row.

    Used to stop retrying tasks that will never make it.
    """

    status = models.CharField(max_length=16, default=STATUS_PENDING)
    """String token with task status; see above.
    """

    error = models.TextField(blank=True)
    """Text with stack trace, if status is "error" or "broken".
    """

    broken_reason = models.CharField(max_length=128, default='', blank=True)
    """Identifier with reason for this permanent failure.
    """

    log = models.TextField(blank=True)
    """Text with first few KB of logs generated when this task was run.
    """

    class Meta:
        """Sets up indexes for the various types of requests.

        New indexes on this table tend to be quite costly to add (3-4h of
        downtime per collection with 1M docs), but required for queries that
        will run a lot.

        Note:
            all foreign keys and primary keys are indexed by default, so
            there's no need to worry about those.
        """
        indexes = [
            # ensure only one variant of the task exists
            UniqueIndex(fields=['func', 'args']),

            # list through each status type (all dispatch)
            models.Index(fields=['status']),

            # stats for last minute
            models.Index(fields=['date_finished']),

            # for main dispatch loop
            models.Index(fields=['func', 'status']),

            # for dispatching in reverse order
            models.Index(fields=['status', 'date_modified']),

            # for retrying all walks, in order
            models.Index(fields=['func', 'date_modified']),

            # for task admin and browsing errors
            models.Index(fields=['broken_reason']),

            # for the 5M task matrix in statistics
            models.Index(fields=['func', 'date_started', 'date_finished']),

            # for selecting outdated tasks
            models.Index(fields=['func', 'version']),

            # for selecting errors to retry
            models.Index(fields=['status', 'fail_count']),
        ]

    class PartitioningMeta:
        """Partition the Tasks table by func, args. This means these become part
        of the primary key, and will be part of all foreign keys."""

        method = PostgresPartitioningMethod.HASH
        key = ["func", "args"]

    def __str__(self):
        """String representation for a Task contains its name, args and status.
        """
        deps = ''
        prev_set = self.prev_set.all()
        prev_ids = ', '.join(str(t.prev.pk) for t in prev_set)
        deps = '; depends on ' + prev_ids if prev_ids else ''
        the_args = str([truncatechars(str(x), 12) for x in self.args])
        return f'Task #{self.pk} {self.func}({the_args}{deps}) [{self.status}]'

    __repr__ = __str__

    def update(self, status=None, error=None, broken_reason=None, log=None, version=None):
        """Helper method to update multiple fields at once, without saving.

        This method also truncates our Text fields to decent limits, so it's
        preferred to use this instead of the fields directly.

        Args:
            status: field to set, if not None
            error: field to set, if not None
            broken_reason: field to set, if not None
            log: field to set, if not None
            version: field to set, if not None
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

        old_version = self.version
        if version is not None:
            self.version = version

        if status is not None:
            self.status = status

        if error is not None:
            self.error = _escape(error)[:2**13]  # 8k of error screen
        if broken_reason is not None:
            self.broken_reason = _escape(broken_reason)[:2**12]  # 4k reason
        if log is not None:
            self.log = _escape(log)[:2**14]  # 16k of log space

        # Increment fail_count only if we ran the same version and still got a bad status code.
        # Reset the fail count only when status is success, or if the version changed.
        if self.status == self.STATUS_SUCCESS or old_version != self.version:
            self.fail_count = 0
        elif self.status in [self.STATUS_BROKEN, self.STATUS_ERROR]:
            self.fail_count = self.fail_count + 1

    def size(self):
        """Returns task size in bytes.
        Includes blob argument size, JSON argument size, and all dependency result blob sizes, all added up.
        """
        s = len(json.dumps(self.args))
        if self.blob_arg:
            s += self.blob_arg.size

        for dep in self.prev_set.all():
            if dep.prev.result:
                s += dep.prev.result.size

        return s

    @property
    def next_set(self):
        next_deps = list(TaskDependency.objects.filter(prev_func=self.func, prev_args=self.args).all())
        if not next_deps:
            return Task.objects.none()
        _filter = functools.reduce(
            operator.or_,
            (Q(func=d.next_func, args=d.next_args) for d in next_deps)
        )
        return Task.objects.filter(_filter)

    @property
    def prev_set(self):
        prev_deps = list(TaskDependency.objects.filter(next_func=self.func, next_args=self.args).all())
        if not prev_deps:
            return Task.objects.none()

        _filter = functools.reduce(
            operator.or_,
            (Q(func=d.prev_func, args=d.prev_args) for d in prev_deps)
        )
        return Task.objects.filter(_filter)


class TaskDependency(models.Model):
    """Database model for tracking which Tasks depend on which.
    This works like a simple M2M relationship - but we have extra metadata
    (dep. variable name).
    """

    prev_func = models.CharField(max_length=1024)
    prev_args = JSONField()
    next_func = models.CharField(max_length=1024)
    next_args = JSONField()

    @property
    def next(self):
        return Task.objects.get(func=self.next_func, args=self.next_args)

    @property
    def prev(self):
        return Task.objects.get(func=self.prev_func, args=self.prev_args)

    name = models.CharField(max_length=1024)
    """ a string used to identify the kwarg name of this dependency"""

    class Meta:
        unique_together = ('prev_func', 'next_func', 'prev_args', 'next_args', 'name')
        verbose_name_plural = 'task dependencies_p'

    def __str__(self):
        """String representation for a TaskDependency contains both task IDs
        and an arrow.
        """
        return f'{self.prev} -> {self.next}'

    __repr__ = __str__
