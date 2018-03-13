import pytest
from fixtures import TESTDATA, CollectionApiClient

pytestmark = [pytest.mark.django_db]


def test_digest_with_broken_dependency(fakedata, taskmanager, client):
    collection = fakedata.collection()
    mof1_1992_233 = TESTDATA / 'disk-files/broken.pdf'
    with mof1_1992_233.open('rb') as f:
        blob = fakedata.blob(f.read())
    assert blob.mime_type == 'application/pdf'
    fakedata.file(collection.root_directory, 'broken.pdf', blob)

    taskmanager.run()

    api = CollectionApiClient(collection, client)
    digest = api.get_digest(blob.pk)['content']

    assert digest['md5'] == 'f6e0d13c5c3aaab75b4febced3e72ae0'
    assert digest['size'] == 1000
    assert digest['text'] is None
    assert digest['broken'] == ['tika_http_422']
