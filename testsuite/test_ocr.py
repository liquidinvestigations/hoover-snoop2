import pytest
from fixtures import TESTDATA, CollectionApiClient
from snoop.data import ocr

pytestmark = [pytest.mark.django_db]


def test_pdf_ocr(fakedata, taskmanager, client):
    ocr1_path = TESTDATA.parent / 'ocr/one'
    ocr.create_ocr_source('ocr1', ocr1_path)

    root = fakedata.init()
    mof1_1992_233 = TESTDATA / 'disk-files/pdf-for-ocr/mof1_1992_233.pdf'
    with mof1_1992_233.open('rb') as f:
        blob = fakedata.blob(f.read())
    fakedata.file(root, 'mof1_1992_233.pdf', blob)

    taskmanager.run()

    api = CollectionApiClient(client)
    digest = api.get_digest(blob.pk)['content']
    assert "Hotărlre privind stabilirea cantităţii de gaze" in digest['text']

    ocr_pdf = ocr1_path / 'foo/bar/f/d/fd41b8f1fe19c151517b3cda2a615fa8.pdf'
    with ocr_pdf.open('rb') as f:
        ocr_pdf_data = f.read()

    resp = client.get(f'/collection/{blob.pk}/ocr/ocr1/')
    assert b''.join(resp.streaming_content) == ocr_pdf_data
    assert resp['Content-Type'] == 'application/pdf'


def test_txt_ocr(fakedata, taskmanager, client):
    ocr2_path = TESTDATA.parent / 'ocr/two'
    ocr.create_ocr_source('ocr2', ocr2_path)
    ocr.dispatch_ocr_tasks()
    taskmanager.run()

    mof1_1992_233 = TESTDATA / 'disk-files/pdf-for-ocr/mof1_1992_233.pdf'
    with mof1_1992_233.open('rb') as f:
        blob = fakedata.blob(f.read())

    [(source, ocrtext)] = ocr.ocr_texts_for_blob(blob)
    assert "totally different" in ocrtext
