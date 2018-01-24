import logging
from collections import deque
import pytest
from snoop.data import tasks
from snoop.data import models

logging.getLogger('celery').setLevel(logging.WARNING)
log = logging.getLogger(__name__)


class TaskManager:

    def __init__(self):
        self.queue = deque()

    def add(self, task_pk):
        self.queue.append(task_pk)

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
    monkeypatch.setattr(tasks.laterz_shaorma, 'delay', taskmanager.add)
    return taskmanager
