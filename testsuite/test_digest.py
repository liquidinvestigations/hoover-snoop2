import pytest
from fixtures import TESTDATA, CollectionApiClient

pytestmark = [pytest.mark.django_db]


def test_digest_with_broken_dependency(fakedata, taskmanager, client):
    root_directory = fakedata.init()
    mof1_1992_233 = TESTDATA / 'disk-files/broken.pdf'
    with mof1_1992_233.open('rb') as f:
        blob = fakedata.blob(f.read())
    assert blob.mime_type == 'application/pdf'
    fakedata.file(root_directory, 'broken.pdf', blob)

    taskmanager.run()

    api = CollectionApiClient(client)
    digest = api.get_digest(blob.pk)['content']

    assert digest['md5'] == 'f6e0d13c5c3aaab75b4febced3e72ae0'
    assert digest['size'] == 1000
    assert digest['text'] is None
    assert digest['broken'] == ['tika_http_422']


def test_digest_msg(fakedata, taskmanager, client):
    root_directory = fakedata.init()
    msg = TESTDATA / 'msg-5-outlook/DISEARĂ-Te-așteptăm-la-discuția-despre-finanțarea-culturii.msg'
    with msg.open('rb') as f:
        blob = fakedata.blob(f.read())
    msg_file = fakedata.file(root_directory, 'the.msg', blob)

    taskmanager.run()

    msg_file.refresh_from_db()
    api = CollectionApiClient(client)
    digest = api.get_digest(msg_file.blob.pk)['content']

    assert digest['content-type'] == 'application/vnd.ms-outlook'
    assert digest['filename'] == 'the.msg'
    assert digest['filetype'] == 'email'
    assert digest['md5'] == '38385c4487719fa9dd0fb695d3aad0ee'
    assert digest['sha1'] == '90548132e18bfc3088e81918bbcaf887a68c6acc'
    assert digest['size'] == 19968


def test_digest_entity_detection(fakedata, taskmanager, monkeypatch):
    data_param = {}

    def detect_entities_mock(id, data):
        data_param['filename'] = data['filename']

    root_directory = fakedata.init()

    txt_file = TESTDATA / 'disk-files' / 'pdf-doc-txt' / 'easychair.txt'
    with txt_file.open('rb') as f:
        blob = fakedata.blob(f.read())

    assert blob.mime_type == 'text/plain'

    fakedata.file(root_directory, 'easychair.txt', blob)

    import snoop.data.analyzers.entities as entities
    monkeypatch.setattr(entities, 'detect_text_entities', detect_entities_mock)

    taskmanager.run()

    assert data_param['filename'] == 'easychair.txt'
