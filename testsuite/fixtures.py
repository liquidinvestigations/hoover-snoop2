from pathlib import Path
from django.utils import timezone
from django.conf import settings
from snoop.data import models
from snoop.data import filesystem
from snoop.data import indexing

TESTDATA = Path(settings.SNOOP_TESTDATA) / 'data'


class FakeData:

    def collection(self, name='testdata'):
        collection = models.Collection.objects.create(
            name=name,
            root='',
        )
        collection.directory_set.create()
        indexing.resetindex(collection.name)
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

    def get_digest(self, blob_hash):
        return self.get(f'/{blob_hash}/json')

    def get_locations(self, blob_hash):
        return self.get(f'/{blob_hash}/locations')
