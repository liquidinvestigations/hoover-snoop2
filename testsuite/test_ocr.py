import pytest
from fixtures import TESTDATA, CollectionApiClient
from snoop.data import ocr

pytestmark = [pytest.mark.django_db]


def test_ocr(fakedata, taskmanager, client):
    ocr1_path = TESTDATA.parent / 'ocr/one'
    ocr.create_ocr_source('ocr1', ocr1_path)

    collection = fakedata.collection()
    mof1_1992_233 = TESTDATA / 'disk-files/pdf-for-ocr/mof1_1992_233.pdf'
    with mof1_1992_233.open('rb') as f:
        blob = fakedata.blob(f.read())
    fakedata.file(collection.root_directory, 'mof1_1992_233.pdf', blob)

    taskmanager.run()

    api = CollectionApiClient(collection, client)
    digest = api.get_digest(blob.pk)['content']
    assert "Hotărlre privind stabilirea cantităţii de gaze" in digest['text']

    ocr_pdf = ocr1_path / 'foo/bar/f/d/41b8f1fe19c151517b3cda2a615fa8.pdf'
    with ocr_pdf.open('rb') as f:
        ocr_pdf_data = f.read()

    resp = client.get(f'/collections/testdata/{blob.pk}/ocr/ocr1/')
    assert b''.join(resp.streaming_content) == ocr_pdf_data
    assert resp['Content-Type'] == 'application/pdf'
