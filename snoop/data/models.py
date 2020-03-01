import string
from contextlib import contextmanager
from pathlib import Path
import tempfile
import hashlib
from django.db import models
from django.conf import settings
from django.template.defaultfilters import truncatechars
from django.contrib.postgres.fields import JSONField
from django.core.exceptions import ObjectDoesNotExist
from .magic import Magic, looks_like_email, looks_like_emlx_email, \
    looks_like_mbox

from . import collections


def blob_root():
    col = collections.current()
    return Path(settings.SNOOP_BLOB_STORAGE) / col.name


def blob_repo_path(sha3_256):
    return blob_root() / sha3_256[:2] / sha3_256[2:4] / sha3_256[4:]


def chunks(file, blocksize=65536):
    while True:
        data = file.read(blocksize)
        if not data:
            return
        yield data


class BlobWriter:

    def __init__(self, file):
        self.file = file
        self.hashes = {
            'md5': hashlib.md5(),
            'sha1': hashlib.sha1(),
            'sha3_256': hashlib.sha3_256(),
            'sha256': hashlib.sha256(),
        }
        self.magic = Magic()
        self.size = 0

    def write(self, chunk):
        for h in self.hashes.values():
            h.update(chunk)
        self.magic.update(chunk)
        self.file.write(chunk)
        self.size += len(chunk)

    def finish(self):
        self.magic.finish()
        fields = {
            name: hash.hexdigest()
            for name, hash in self.hashes.items()
        }
        fields['mime_type'] = self.magic.mime_type
        fields['mime_encoding'] = self.magic.mime_encoding
        fields['magic'] = self.magic.magic_output
        fields['size'] = self.size
        return fields


class Blob(models.Model):
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
        return self.pk

    __repr__ = __str__

    @property
    def content_type(self):
        if self.mime_type.startswith('text/'):
            return f"{self.mime_type}; charset={self.mime_encoding}"

        return self.mime_type

    def path(self):
        return blob_repo_path(self.pk)

    @classmethod
    @contextmanager
    def create(cls):
        blob_tmp = blob_root() / 'tmp'
        blob_tmp.mkdir(exist_ok=True, parents=True)

        with tempfile.NamedTemporaryFile(dir=blob_tmp, delete=False) as f:
            writer = BlobWriter(f)
            yield writer

        fields = writer.finish()
        pk = fields.pop('sha3_256')

        blob_path = blob_repo_path(pk)
        blob_path.parent.mkdir(exist_ok=True, parents=True)
        temp_blob_path = Path(f.name)
        temp_blob_path.chmod(0o444)
        temp_blob_path.rename(blob_path)

        if fields['mime_type'].startswith('text/'):
            if looks_like_email(blob_path):
                if looks_like_emlx_email(blob_path):
                    fields['mime_type'] = 'message/x-emlx'
                elif looks_like_mbox(blob_path):
                    fields['mime_type'] = 'application/mbox'
                else:
                    fields['mime_type'] = 'message/rfc822'

        if fields['magic'].startswith('Microsoft Outlook email folder'):
            fields['mime_type'] = 'application/x-hoover-pst'

        (blob, _) = cls.objects.get_or_create(pk=pk, defaults=fields)
        writer.blob = blob

    @classmethod
    def create_from_bytes(cls, data):
        sha3_256 = hashlib.sha3_256()
        sha3_256.update(data)

        try:
            return Blob.objects.get(pk=sha3_256.hexdigest())

        except ObjectDoesNotExist:
            with cls.create() as writer:
                writer.write(data)

            return writer.blob

    @classmethod
    def create_from_file(cls, path):
        file_sha3_256 = hashlib.sha3_256()
        with open(path, 'rb') as f:
            for block in chunks(f):
                file_sha3_256.update(block)

        try:
            return Blob.objects.get(pk=file_sha3_256.hexdigest())

        except ObjectDoesNotExist:
            with cls.create() as writer:
                with open(path, 'rb') as f:
                    for block in chunks(f):
                        writer.write(block)

            return writer.blob

    def open(self, encoding=None):
        if encoding is not None:
            mode = 'r'
        else:
            mode = 'rb'
        return self.path().open(mode, encoding=encoding)


class Directory(models.Model):
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
        return cls.objects.filter(
            parent_directory__isnull=True,
            container_file__isnull=True
        ).first()

    @property
    def name(self):
        name_bytes = self.name_bytes
        if isinstance(name_bytes, memoryview):
            name_bytes = name_bytes.tobytes()
        return name_bytes.decode('utf8', errors='surrogateescape')

    @property
    def parent(self):
        return self.parent_directory or self.container_file

    def ancestry(item):
        while item:
            yield item
            item = item.parent

    def __str__(self):
        return ''.join(reversed([f'{item.name}/' for item in self.ancestry()]))

    __repr__ = __str__


class File(models.Model):
    name_bytes = models.BinaryField(max_length=1024, blank=True)
    parent_directory = models.ForeignKey(
        Directory,
        on_delete=models.CASCADE,
        related_name='child_file_set',
    )
    ctime = models.DateTimeField()
    mtime = models.DateTimeField()
    size = models.BigIntegerField()
    original = models.ForeignKey(Blob, on_delete=models.DO_NOTHING,
                                 related_name='+')
    blob = models.ForeignKey(Blob, on_delete=models.DO_NOTHING)

    date_created = models.DateTimeField(auto_now_add=True)
    date_modified = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ('parent_directory', 'name_bytes')

    @property
    def name(self):
        name_bytes = self.name_bytes
        if isinstance(name_bytes, memoryview):
            name_bytes = name_bytes.tobytes()
        return name_bytes.decode('utf8', errors='surrogateescape')

    def __str__(self):
        return truncatechars(self.name, 80)

    __repr__ = __str__

    @property
    def parent(self):
        return self.parent_directory


class Task(models.Model):
    STATUS_PENDING = 'pending'
    STATUS_SUCCESS = 'success'
    STATUS_ERROR = 'error'
    STATUS_DEFERRED = 'deferred'
    STATUS_BROKEN = 'broken'

    func = models.CharField(max_length=1024)
    blob_arg = models.ForeignKey(Blob, null=True, blank=True,
                                 on_delete=models.DO_NOTHING,
                                 related_name='+')
    args = JSONField()
    result = models.ForeignKey(Blob, null=True, blank=True,
                               on_delete=models.DO_NOTHING)

    # these fields are used for logging and debugging, not for dispatching
    date_created = models.DateTimeField(auto_now_add=True)
    date_modified = models.DateTimeField(auto_now=True)
    date_started = models.DateTimeField(null=True, blank=True)
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
        deps = ''
        prev_set = self.prev_set.all()
        prev_ids = ', '.join(str(t.prev.pk) for t in prev_set)
        deps = '; depends on ' + prev_ids if prev_ids else ''
        return f'#{self.pk} {self.func}({self.args}{deps}) [{self.status}]'

    __repr__ = __str__

    def update(self, status, error, broken_reason, log):
        def _escape(s):
            def _translate(x):
                if x in string.printable:
                    return x
                return f'\\{ord(x)}'
            return "".join(map(_translate, s))

        self.status = status
        self.error = _escape(error)
        self.broken_reason = _escape(broken_reason)
        self.log = _escape(log)


class TaskDependency(models.Model):
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
        return f'{self.prev} -> {self.next}'

    __repr__ = __str__


class Digest(models.Model):
    blob = models.OneToOneField(Blob, on_delete=models.DO_NOTHING)
    result = models.ForeignKey(
        Blob,
        on_delete=models.DO_NOTHING,
        related_name='+',
    )

    date_created = models.DateTimeField(auto_now_add=True)
    date_modified = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=['date_modified']),
        ]


class OcrSource(models.Model):
    name = models.CharField(max_length=1024)
    root = models.CharField(max_length=4096)


class OcrDocument(models.Model):
    source = models.ForeignKey(OcrSource, on_delete=models.DO_NOTHING)
    original_hash = models.CharField(max_length=64, db_index=True)
    ocr = models.ForeignKey(Blob, on_delete=models.DO_NOTHING)
    text = models.ForeignKey(Blob, on_delete=models.DO_NOTHING,
                             related_name='+')

    class Meta:
        unique_together = ('source', 'original_hash')
