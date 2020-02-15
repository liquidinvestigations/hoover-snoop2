from pathlib import Path
from django.utils import timezone
from django.conf import settings
from snoop.data import models
from snoop.data import filesystem
from snoop.data import indexing
from snoop.data import collections

TESTDATA = Path(settings.SNOOP_TESTDATA) / 'data'


class FakeData:

    def init(self):
        col = collections.current()
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
        url = f'/collection{url}'
        resp = self.client.get(url)
        assert resp.status_code == 200
        return resp.json()

    def get_digest(self, blob_hash):
        return self.get(f'/{blob_hash}/json')

    def get_locations(self, blob_hash):
        return self.get(f'/{blob_hash}/locations')
