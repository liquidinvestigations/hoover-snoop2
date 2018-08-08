import pytest
from fixtures import TESTDATA, CollectionApiClient

pytestmark = [pytest.mark.django_db]


def test_tika_digested(fakedata, taskmanager, client):
    collection = fakedata.collection()
    legea_pdf = TESTDATA / './no-extension/file_doc'
    with legea_pdf.open('rb') as f:
        blob = fakedata.blob(f.read())
    fakedata.file(collection.root_directory, 'file.doc', blob)

    taskmanager.run()

    api = CollectionApiClient(collection, client)
    digest = api.get_digest(blob.pk)['content']

    assert "Colors and Lines to choose" in digest['text']
    assert digest['date'] == '2016-01-13T11:05:00Z'
    assert digest['date-created'] == '2016-01-13T11:00:00Z'
