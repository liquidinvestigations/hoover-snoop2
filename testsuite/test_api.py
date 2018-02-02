import pytest
from django.utils import timezone
from snoop.data import models
from snoop.data import filesystem

pytestmark = [pytest.mark.django_db]


class FakeData:

    def collection(self, name='testdata'):
        collection = models.Collection.objects.create(name=name, root='')
        collection.directory_set.create()
        return collection

    def blob(self, data):
        return models.Blob.create_from_bytes(data)

    def directory(self, parent, name):
        directory = models.Directory.objects.create(
            collection=parent.collection,
            parent_directory=parent,
            name=name,
        )
        return directory

    def file(self, parent, name, blob):
        now = timezone.now()
        file = models.File.objects.create(
            collection=parent.collection,
            parent_directory=parent,
            name=name,
            ctime=now,
            mtime=now,
            size=blob.size,
            original=blob,
            blob=blob,
        )
        filesystem.handle_file.laterz(file.pk)
        return file


class CollectionApiClient:

    def __init__(self, collection, client):
        self.collection = collection
        self.client = client

    def get(self, url):
        url = f'/collections/{self.collection.name}{url}'
        resp = self.client.get(url)
        assert resp.status_code == 200
        return resp.json()

    def get_locations(self, blob_hash):
        return self.get(f'/{blob_hash}/locations')


@pytest.fixture
def fakedata():
    return FakeData()


def test_blob_locations(client, fakedata, taskmanager):
    collection = fakedata.collection()
    dir1 = fakedata.directory(collection.root_directory, 'dir1')
    dir2 = fakedata.directory(collection.root_directory, 'dir2')
    blob = fakedata.blob(b'hello world')
    fakedata.file(dir1, 'foo', blob)
    fakedata.file(dir2, 'bar', blob)

    taskmanager.run()

    def directory_id(directory):
        return f'_directory_{directory.pk}'

    api = CollectionApiClient(collection, client)
    resp = api.get_locations(blob.pk)
    assert resp['locations'] == [
        {
            'filename': 'foo',
            'parent_id': directory_id(dir1),
            'parent_path': '/dir1',
        },
        {
            'filename': 'bar',
            'parent_id': directory_id(dir2),
            'parent_path': '/dir2',
        },
    ]
