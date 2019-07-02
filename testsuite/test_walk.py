from pathlib import Path
import tempfile

from django.conf import settings
import pytest

from snoop.data import dispatcher
from snoop.data import filesystem
from snoop.data import models

TESTDATA = Path(settings.SNOOP_TESTDATA)

pytestmark = [pytest.mark.django_db]


def test_walk(taskmanager, monkeypatch):
    root_path = Path(settings.SNOOP_COLLECTION_ROOT) / 'emlx-4-missing-part'
    monkeypatch.setattr(settings, 'SNOOP_COLLECTION_ROOT', root_path)
    root = models.Directory.objects.create()

    filesystem.walk(root.pk)

    [file] = models.File.objects.all()
    hash = '442e8939e3e367c4263738bbb29e9360a17334279f1ecef67fa9d437c31804ca'
    assert file.original.pk == hash
    assert file.blob.pk == hash

    [task_pk] = taskmanager.queue
    task = models.Task.objects.get(pk=task_pk)
    assert task.func == 'filesystem.handle_file'
    assert task.args == [file.pk]


def test_smashed_filename(taskmanager, monkeypatch):
    with tempfile.TemporaryDirectory() as dir:
        monkeypatch.setattr(settings, 'SNOOP_COLLECTION_ROOT', dir)
        root = models.Directory.objects.create()

        broken_name = 'modifi\udce9.txt'
        with (Path(dir) / broken_name).open('w') as f:
            f.write('hello world\n')

        filesystem.walk(root.pk)

    [file] = models.File.objects.all()
    hash = 'a8009a7a528d87778c356da3a55d964719e818666a04e4f960c9e2439e35f138'
    assert file.original.pk == hash
    assert file.name == broken_name


def test_children_of_archives_in_multiple_locations(taskmanager, monkeypatch):
    root_path = Path(settings.SNOOP_COLLECTION_ROOT) / 'zip-in-multiple-locations'
    monkeypatch.setattr(settings, 'SNOOP_COLLECTION_ROOT', root_path)
    models.Directory.objects.create()

    # filesystem.walk(root.pk)
    dispatcher.run_dispatcher()
    taskmanager.run(limit=10000)

    files = list(models.File.objects.all())
    dirs = list(models.Directory.objects.all())

    [z1, c1, z2, c2] = sorted(files, key=lambda x: str(x.parent) + str(x))
    assert z1.blob == z2.blob
    assert c1.blob == c2.blob

    assert [(str(x), list(x.child_file_set.all())) for x in sorted(dirs, key=str)] == [
        ('/', []),
        ('/location-1/', [z1]),
        ('/location-1/parent.zip//', []),
        ('/location-1/parent.zip//parent/', [c1]),
        ('/location-2/', [z2]),
        ('/location-2/parent.zip//', []),
        ('/location-2/parent.zip//parent/', [c2])
    ]
    d1 = sorted(dirs, key=str)[2]
    d2 = sorted(dirs, key=str)[5]
    assert d1.child_directory_set.all()[0].parent == d1
    assert d1.parent == z1
    assert d2.child_directory_set.all()[0].parent == d2
    assert d2.parent == z2
