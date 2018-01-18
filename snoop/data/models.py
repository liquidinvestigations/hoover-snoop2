from contextlib import contextmanager
from pathlib import Path
import tempfile
import hashlib
from django.db import models
from django.conf import settings
from django.template.defaultfilters import truncatechars
from django.contrib.postgres.fields import JSONField
from .magic import Magic, looks_like_email, looks_like_emlx_email

BLOB_ROOT = Path(settings.SNOOP_BLOB_STORAGE)
BLOB_TMP = BLOB_ROOT / 'tmp'


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

    def write(self, chunk):
        for h in self.hashes.values():
            h.update(chunk)
        self.magic.update(chunk)
        self.file.write(chunk)

    def finish(self):
        self.magic.finish()
        fields = {
            name: hash.hexdigest()
            for name, hash in self.hashes.items()
        }
        fields['mime_type'] = self.magic.mime_type
        fields['mime_encoding'] = self.magic.mime_encoding
        fields['magic'] = self.magic.magic_output
        return fields


class Blob(models.Model):
    sha3_256 = models.CharField(max_length=64, primary_key=True)
    sha256 = models.CharField(max_length=64, db_index=True)
    sha1 = models.CharField(max_length=40, db_index=True)
    md5 = models.CharField(max_length=32, db_index=True)

    magic = models.CharField(max_length=4096)
    mime_type = models.CharField(max_length=1024)
    mime_encoding = models.CharField(max_length=1024)

    date_created = models.DateTimeField(auto_now_add=True)
    date_modified = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.pk

    def path(self):
        return BLOB_ROOT / self.pk

    @classmethod
    @contextmanager
    def create(cls):
        BLOB_ROOT.mkdir(exist_ok=True)
        BLOB_TMP.mkdir(exist_ok=True)

        with tempfile.NamedTemporaryFile(dir=BLOB_TMP, delete=False) as f:
            writer = BlobWriter(f)
            yield writer

        fields = writer.finish()
        pk = fields.pop('sha3_256')

        blob_path = BLOB_ROOT / pk
        Path(f.name).rename(blob_path)

        if fields['mime_type'].startswith('text/'):
            if looks_like_email(blob_path):
                if looks_like_emlx_email(blob_path):
                    fields['mime_type'] = 'message/x-emlx'
                else:
                    fields['mime_type'] = 'message/rfc822'

        if fields['magic'].startswith('Microsoft Outlook email folder'):
            fields['mime_type'] = 'application/x-hoover-pst'

        (blob, _) = cls.objects.get_or_create(pk=pk, defaults=fields)
        writer.blob = blob

    @classmethod
    def create_from_file(cls, path):
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


class Collection(models.Model):
    name = models.CharField(max_length=128, unique=True)
    root = models.CharField(max_length=4096)

    date_created = models.DateTimeField(auto_now_add=True)
    date_modified = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name


class Directory(models.Model):
    collection = models.ForeignKey(Collection, on_delete=models.DO_NOTHING)
    name = models.CharField(max_length=255, blank=True)
    parent_directory = models.ForeignKey(
        'Directory',
        null=True,
        on_delete=models.DO_NOTHING,
        related_name='child_directory_set',
    )
    container_file = models.ForeignKey(
        'File',
        null=True,
        on_delete=models.DO_NOTHING,
        related_name='child_directory_set',
    )

    date_created = models.DateTimeField(auto_now_add=True)
    date_modified = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ('parent_directory', 'name')

    def __str__(self):
        return f'{self.name}/'


class File(models.Model):
    collection = models.ForeignKey(Collection, on_delete=models.DO_NOTHING)
    name = models.CharField(max_length=255)
    parent_directory = models.ForeignKey(
        Directory,
        on_delete=models.DO_NOTHING,
        related_name='child_file_set',
    )
    ctime = models.DateTimeField()
    mtime = models.DateTimeField()
    size = models.IntegerField()
    blob = models.ForeignKey(Blob, on_delete=models.DO_NOTHING)

    date_created = models.DateTimeField(auto_now_add=True)
    date_modified = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ('parent_directory', 'name')

    def __str__(self):
        return truncatechars(self.name, 80)


class Task(models.Model):
    STATUS_PENDING = 'pending'
    STATUS_SUCCESS = 'success'
    STATUS_ERROR = 'error'

    func = models.CharField(max_length=1024)
    blob_arg = models.ForeignKey(Blob, null=True, on_delete=models.DO_NOTHING,
                                 related_name='+')
    args = JSONField()
    result = models.ForeignKey(Blob, null=True, on_delete=models.DO_NOTHING)

    # these fields are used for logging and debugging, not for dispatching
    date_created = models.DateTimeField(auto_now_add=True)
    date_modified = models.DateTimeField(auto_now=True)
    date_started = models.DateTimeField(null=True)
    date_finished = models.DateTimeField(null=True)
    worker = models.CharField(max_length=4096, blank=True)

    status = models.CharField(max_length=16, default=STATUS_PENDING)
    error = models.TextField(blank=True)
    traceback = models.TextField(blank=True)

    class Meta:
        unique_together = ('func', 'args')

    def __str__(self):
        deps = ''
        prev_set = self.prev_set.all()
        if prev_set:
            deps = (
                '; depends on ' +
                ', '.join(str(t.prev.pk) for t in prev_set)
            )
        return f'{self.func}({self.args}{deps})'


class TaskDependency(models.Model):
    prev = models.ForeignKey(
        Task,
        on_delete=models.DO_NOTHING,
        related_name='next_set',
    )
    next = models.ForeignKey(
        Task,
        on_delete=models.DO_NOTHING,
        related_name='prev_set',
    )
    name = models.CharField(max_length=1024)

    class Meta:
        unique_together = ('prev', 'next', 'name')

    def __str__(self):
        return f'{self.prev} -> {self.next}'


class Digest(models.Model):
    collection = models.ForeignKey(Collection, on_delete=models.DO_NOTHING)
    blob = models.ForeignKey(Blob, on_delete=models.DO_NOTHING)
    result = models.ForeignKey(
        Blob,
        on_delete=models.DO_NOTHING,
        related_name='+',
    )

    date_created = models.DateTimeField(auto_now_add=True)
    date_modified = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ('collection', 'blob')
