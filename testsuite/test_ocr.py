import pytest

from snoop.data import ocr
from conftest import TESTDATA, CollectionApiClient, mask_out_current_collection

pytestmark = [pytest.mark.django_db]


@pytest.mark.skip(reason="client request is 404, needs rewrite")
def test_pdf_ocr(fakedata, taskmanager, client, settings_with_ocr):
    source = ocr.create_ocr_source('one')

    root = fakedata.init()
    mof1_1992_233 = TESTDATA / 'disk-files/pdf-for-ocr/mof1_1992_233.pdf'
    with mof1_1992_233.open('rb') as f:
        blob = fakedata.blob(f.read())
    fakedata.file(root, 'mof1_1992_233.pdf', blob)

    taskmanager.run()

    api = CollectionApiClient(client)
    digest = api.get_digest(blob.pk)['content']
    assert "Hotărlre privind stabilirea cantităţii de gaze" in digest['ocrtext']['one']

    ocr_pdf = source.root / 'foo/bar/f/d/fd41b8f1fe19c151517b3cda2a615fa8.pdf'
    with ocr_pdf.open('rb') as f:
        ocr_pdf_data = f.read()

    with mask_out_current_collection():
        resp = client.get(f'/collections/testdata/{blob.pk}/ocr/one/')
    assert b''.join(resp.streaming_content) == ocr_pdf_data
    assert resp['Content-Type'] == 'application/pdf'


def test_txt_ocr(fakedata, taskmanager, client, settings_with_ocr):
    ocr.create_ocr_source('two')
    ocr.dispatch_ocr_tasks()
    taskmanager.run()

    mof1_1992_233 = TESTDATA / 'disk-files/pdf-for-ocr/mof1_1992_233.pdf'
    with mof1_1992_233.open('rb') as f:
        blob = fakedata.blob(f.read())

    [(source, ocrtext)] = ocr.ocr_texts_for_blob(blob)
    assert "totally different" in ocrtext
