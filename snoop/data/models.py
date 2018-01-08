from django.db import models


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


class Collection(models.Model):
    name = models.CharField(max_length=128, unique=True)
    root = models.CharField(max_length=4096)

    date_created = models.DateTimeField(auto_now_add=True)
    date_modified = models.DateTimeField(auto_now=True)


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
        return self.name


class Task(models.Model):
    func = models.CharField(max_length=1024)
    args = models.CharField(max_length=4096)
    result = models.ForeignKey(Blob, null=True, on_delete=models.DO_NOTHING)

    # these fields are used for logging and debugging, not for dispatching
    date_created = models.DateTimeField(auto_now_add=True)
    date_modified = models.DateTimeField(auto_now=True)
    date_started = models.DateTimeField(null=True)
    date_finished = models.DateTimeField(null=True)
    worker = models.CharField(max_length=4096, blank=True)

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

    class Meta:
        unique_together = ('collection', 'blob')
