from pathlib import Path
import logging
from collections import deque
from contextlib import contextmanager

import pytest
from django.conf import settings
from django.utils import timezone
from django.db import transaction
from ranged_response import RangedFileResponse

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
    if not request.node.get_closest_marker('django_db'):
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
    testdata = collections.get_all()['testdata']
    with testdata.set_current():
        yield


@pytest.fixture
def settings_with_thumbnails():
    settings.SNOOP_THUMBNAIL_URL = settings.ORIG_SNOOP_THUMBNAIL_URL
    yield
    settings.SNOOP_THUMBNAIL_URL = None


@pytest.fixture
def settings_with_object_detection():
    settings.SNOOP_OBJECT_DETECTION_URL = settings.ORIG_SNOOP_OBJECT_DETECTION_URL
    yield
    settings.SNOOP_OBJECT_DETECTION_URL = None


@pytest.fixture
def settings_with_entities():
    settings.EXTRACT_ENTITIES = settings.ORIG_EXTRACT_ENTITIES
    settings.DETECT_LANGUAGE = settings.ORIG_DETECT_LANGUAGE
    yield
    settings.EXTRACT_ENTITIES = False
    settings.DETECT_LANGUAGE = False


@pytest.fixture
def settings_with_translation():
    settings.TRANSLATION_URL = settings.ORIG_TRANSLATION_URL
    yield
    settings.TRANSLATION_URL = None


@pytest.fixture
def settings_with_ocr():
    settings.OCR_ENABLED = settings.ORIG_OCR_ENABLED
    yield
    settings.OCR_ENABLED = False


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

    def run(self, limit=1600):
        count = 0
        max_count = limit * 50
        task_pks = set()
        while self.queue:
            count += 1
            task_pk = self.queue.popleft()
            task_pks.add(task_pk)
            task = (
                models.Task
                .objects.using(self.collection.db_alias)
                .get(pk=task_pk)
            )
            log.debug(f"TaskManager #{count}: {task}")
            with mask_out_current_collection():
                tasks.laterz_snoop_task(self.collection.name, task_pk)
            if len(task_pks) >= limit:
                raise RuntimeError(f"Task count limit exceeded (max task count: {limit})")
            if count >= max_count:
                raise RuntimeError(f"Task limit exceeded (max exec count: {max_count})")
        return len(task_pks)


@pytest.fixture
def taskmanager(monkeypatch):
    taskmanager = TaskManager(collections.get_all()['testdata'])
    monkeypatch.setattr(tasks, 'queue_task', taskmanager.add)
    monkeypatch.setattr(tasks, 'get_rabbitmq_queue_length', lambda _: 0)
    monkeypatch.setattr(tasks, 'single_task_running', lambda _: True)
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

        bucket = collections.current().name
        # if settings.BLOBS_S3.bucket_exists(bucket):
        #     for obj in settings.BLOBS_S3.list_objects(bucket, prefix='/', recursive=True):
        #         settings.BLOBS_S3.remove_object(bucket, obj.object_name)
        #     settings.BLOBS_S3.remove_bucket(bucket)
        if not settings.BLOBS_S3.bucket_exists(bucket):
            settings.BLOBS_S3.make_bucket(bucket)

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

    def get(self, url, params={}):
        col = collections.current()
        url = f'/collections/{col.name}{url}'
        with mask_out_current_collection():
            resp = self.client.get(url)
        assert resp.status_code == 200
        return resp.json()

    def get_digest(self, blob_hash, children_page=1):
        return self.get(f'/{blob_hash}/json?children_page={children_page}')

    def get_locations(self, blob_hash, page=1):
        return self.get(f'/{blob_hash}/locations?page={page}')

    def get_download(self, blob_hash, filename, range=False):
        headers = {}
        if range:
            headers = {'HTTP_RANGE': 'bytes=0-15'}
        col = collections.current()
        with mask_out_current_collection():
            r = self.client.get(f'/collections/{col.name}/{blob_hash}/raw/{filename}', **headers)
            if range:
                assert type(r) is RangedFileResponse
                assert r.status_code == 206
                assert len(r.getvalue()) == 16
            else:
                assert r.status_code == 200
            return r

    def get_thumbnail(self, blob_hash, size):
        col = collections.current()
        url = f'/collections/{col.name}/{blob_hash}/thumbnail/{size}.jpg'
        with mask_out_current_collection():
            resp = self.client.get(url)
        assert resp.status_code == 200
        return resp

    def get_pdf_preview(self, blob_hash, range=False):
        headers = {}
        if range:
            headers = {'HTTP_RANGE': 'bytes=0-15'}
        col = collections.current()
        url = f'/collections/{col.name}/{blob_hash}/pdf-preview'
        with mask_out_current_collection():
            resp = self.client.get(url, **headers)
            if range:
                assert type(resp) is RangedFileResponse
                assert resp.status_code == 206
                assert len(resp.getvalue()) == 16
            else:
                assert resp.status_code == 200
