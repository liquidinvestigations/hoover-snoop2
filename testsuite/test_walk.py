from pathlib import Path
import pytest
import tempfile
from django.conf import settings
from snoop.data import models
from snoop.data import tasks
from snoop.data import filesystem
from conftest import mkdir

TESTDATA = Path(settings.SNOOP_TESTDATA)

pytestmark = [pytest.mark.django_db]


def test_walk(taskmanager):
    collection = models.Collection.objects.create(name='test')
    root = collection.directory_set.create()
    collection.root = TESTDATA / 'data/emlx-4-missing-part'
    collection.save()

    filesystem.walk(root.pk)

    [file] = collection.file_set.all()
    hash = '442e8939e3e367c4263738bbb29e9360a17334279f1ecef67fa9d437c31804ca'
    assert file.original.pk == hash
    assert file.blob is None

    [task_pk] = taskmanager.queue
    task = models.Task.objects.get(pk=task_pk)
    assert task.func == 'filesystem.handle_file'
    assert task.args == [file.pk]


def test_smashed_filename(taskmanager):
    with tempfile.TemporaryDirectory() as dir:
        collection = models.Collection.objects.create(name='test')
        root = collection.directory_set.create()
        collection.root = dir
        collection.save()

        broken_name = 'modifi\udce9.txt'
        with (Path(dir) / broken_name).open('w') as f:
            f.write('hello world\n')

        filesystem.walk(root.pk)

    [file] = collection.file_set.all()
    hash = 'a8009a7a528d87778c356da3a55d964719e818666a04e4f960c9e2439e35f138'
    assert file.original.pk == hash
    assert file.name == broken_name
