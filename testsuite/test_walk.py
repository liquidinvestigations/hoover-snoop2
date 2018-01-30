from pathlib import Path
import pytest
from django.conf import settings
from snoop.data import models
from snoop.data import tasks
from snoop.data import filesystem
from conftest import mkdir

TESTDATA = Path(settings.SNOOP_TESTDATA)

pytestmark = [pytest.mark.django_db]


def test_walk_file(taskmanager):
    collection = models.Collection.objects.create(name='test')
    root = collection.directory_set.create()
    collection.root = TESTDATA / 'data/emlx-4-missing-part'
    collection.save()

    filesystem.walk_file(root.pk, '1498.partial.emlx')

    [file] = collection.file_set.all()
    hash = '442e8939e3e367c4263738bbb29e9360a17334279f1ecef67fa9d437c31804ca'
    assert file.original.pk == hash
    assert file.blob is None

    [task_pk] = taskmanager.queue
    task = models.Task.objects.get(pk=task_pk)
    assert task.func == 'filesystem.handle_file'
    assert task.args == [file.pk]

    with pytest.raises(tasks.ShaormaBroken) as e:
        filesystem.walk_file(root.pk, 'no-such-file')

    assert e.value.reason == 'file_missing'
