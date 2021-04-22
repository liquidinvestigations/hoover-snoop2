import pytest
from snoop.data.analyzers import thumbnails
from conftest import TESTDATA, CollectionApiClient

pytestmark = [pytest.mark.django_db]


def test_thumbnail_job(fakedata, taskmanager, client):
    test_doc = TESTDATA / './no-extension/file_doc'
    with test_doc.open('rb') as f:
        thumbnails.call_thumbnails_service(f, 100)


def test_thumbnail_digested(fakedata, taskmanager, client):
    root = fakedata.init()
    test_doc = TESTDATA / './no-extension/file_doc'
    with test_doc.open('rb') as f:
        blob = fakedata.blob(f.read())

    fakedata.file(root, 'file.doc', blob)

    taskmanager.run()

    api = CollectionApiClient(client)
    digest = api.get_digest(blob.pk)['content']

    print(digest.keys())

    assert digest['has-thumbnails'] is True


def test_thumbnail_api(fakedata, taskmanager, client):
    root = fakedata.init()
    test_pdf = TESTDATA / './no-extension/file_pdf'
    with test_pdf.open('rb') as f:
        blob = fakedata.blob(f.read())

    fakedata.file(root, 'file.pdf', blob)

    taskmanager.run()

    api = CollectionApiClient(client)

    sizes = [100, 200, 400]

    for size in sizes:
        resp = api.get_thumbnail(blob.pk, size)
        print(resp.getheaders())
        assert resp.content_type == 'image/jpeg'
