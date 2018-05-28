import logging
from collections import deque
import pytest
from django.utils import timezone
from snoop.data import tasks
from snoop.data import models
from fixtures import FakeData

logging.getLogger('celery').setLevel(logging.WARNING)
log = logging.getLogger(__name__)


class TaskManager:

    def __init__(self):
        self.queue = deque()

    def add(self, task):
        self.queue.append(task.pk)

    def run(self, limit=100):
        count = 0
        while self.queue:
            count += 1
            task_pk = self.queue.popleft()
            task = models.Task.objects.get(pk=task_pk)
            log.debug(f"TaskManager #{count}: {task}")
            tasks.laterz_shaorma(task_pk)
            if count >= limit:
                raise RuntimeError(f"Task limit exceeded ({limit})")
        return count


@pytest.fixture
def taskmanager(monkeypatch):
    taskmanager = TaskManager()
    monkeypatch.setattr(tasks, 'queue_task', taskmanager.add)
    return taskmanager


@pytest.fixture
def fakedata():
    return FakeData()


def mkdir(parent, name):
    return models.Directory.objects.create(
        collection=parent.collection,
        parent_directory=parent,
        name_bytes=name.encode('utf8'),
    )


def mkfile(parent, name, original):
    now = timezone.now()
    return models.File.objects.create(
        collection=parent.collection,
        parent_directory=parent,
        name_bytes=name.encode('utf8'),
        ctime=now,
        mtime=now,
        size=0,
        original=original,
        blob=original,
    )
