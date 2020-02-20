from pathlib import Path
import logging
from collections import deque
from contextlib import contextmanager

import pytest
from django.conf import settings
from django.utils import timezone
from django.db import transaction

from snoop.data import tasks
from snoop.data import models
from snoop.data import collections
from snoop.data import filesystem
from snoop.data import indexing

logging.getLogger('celery').setLevel(logging.WARNING)
log = logging.getLogger(__name__)

TESTDATA = Path(settings.SNOOP_TESTDATA) / 'data'


@pytest.fixture(autouse=True)
def testdata_transaction(request):
    if not request.keywords._markers.get('django_db'):
        yield
        return

    with transaction.atomic(using='collection_testdata'):
        sid = transaction.savepoint(using='collection_testdata')
        try:
            yield
        finally:
            transaction.savepoint_rollback(sid, using='collection_testdata')


@pytest.fixture(autouse=True)
def testdata_current():
    testdata = collections.ALL['testdata']
    with testdata.set_current():
        yield


@contextmanager
def mask_out_current_collection():
    try:
        col = collections.threadlocal.collection
        collections.threadlocal.collection = None
        yield
    finally:
        collections.threadlocal.collection = col


class TaskManager:

    def __init__(self, collection):
        self.queue = deque()
        self.collection = collection

    def add(self, task):
        self.queue.append(task.pk)

    def run(self, limit=100):
        count = 0
        while self.queue:
            count += 1
            task_pk = self.queue.popleft()
            task = (
                models.Task
                .objects.using(self.collection.db_alias)
                .get(pk=task_pk)
            )
            log.debug(f"TaskManager #{count}: {task}")
            with mask_out_current_collection():
                tasks.laterz_shaorma(self.collection.name, task_pk)
            if count >= limit:
                raise RuntimeError(f"Task limit exceeded ({limit})")
        return count


@pytest.fixture
def taskmanager(monkeypatch):
    taskmanager = TaskManager(collections.ALL['testdata'])
    monkeypatch.setattr(tasks, 'queue_task', taskmanager.add)
    monkeypatch.setattr(tasks, 'has_any_tasks', lambda: False)
    return taskmanager


@pytest.fixture
def fakedata():
    return FakeData()


def mkdir(parent, name):
    return models.Directory.objects.create(
        parent_directory=parent,
        name_bytes=name.encode('utf8'),
    )


def mkfile(parent, name, original):
    now = timezone.now()
    return models.File.objects.create(
        parent_directory=parent,
        name_bytes=name.encode('utf8'),
        ctime=now,
        mtime=now,
        size=0,
        original=original,
        blob=original,
    )


class FakeData:

    def init(self):
        indexing.delete_index()
        indexing.create_index()
        return models.Directory.objects.create()

    def blob(self, data):
        return models.Blob.create_from_bytes(data)

    def directory(self, parent, name):
        directory = parent.child_directory_set.create(
            name_bytes=name.encode('utf8'),
        )
        return directory

    def file(self, parent, name, blob):
        now = timezone.now()
        file = parent.child_file_set.create(
            parent_directory=parent,
            name_bytes=name.encode('utf8'),
            ctime=now,
            mtime=now,
            size=blob.size,
            original=blob,
            blob=blob,
        )
        filesystem.handle_file.laterz(file.pk)
        return file


class CollectionApiClient:

    def __init__(self, client):
        self.client = client

    def get(self, url):
        col = collections.current()
        url = f'/collections/{col.name}{url}'
        with mask_out_current_collection():
            resp = self.client.get(url)
        assert resp.status_code == 200
        return resp.json()

    def get_digest(self, blob_hash):
        return self.get(f'/{blob_hash}/json')

    def get_locations(self, blob_hash):
        return self.get(f'/{blob_hash}/locations')
