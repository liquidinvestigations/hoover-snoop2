import pytest
from snoop.data import collections, models
from conftest import TESTDATA

pytestmark = [pytest.mark.django_db]


def test_tags_api(fakedata, taskmanager, client, django_user_model):
    root = fakedata.init()

    testfile = (TESTDATA / './no-extension/file_pdf')

    with testfile.open('rb') as f:
        blob = fakedata.blob(f.read())

    fakedata.file(root, 'file.pdf', blob)
    taskmanager.run(limit=3000)
    tag = 'test-tag'
    user = django_user_model.objects.create_user(username='test', password='pw')
    create_tag(client, tag, blob.pk, user.username)

    digest = models.Digest.objects.get(blob_id=blob.pk)
    assert models.DocumentUserTag.objects.filter(digest_id=digest.pk).exists()


def create_tag(client, tag, blob_hash, username):
    col = collections.current()
    uuid = 'xyz-123'
    url = f'/collections/{col.name}/{blob_hash}/tags/{username}/{uuid}'
    payload = {
        'tag': tag,
        'public': True
    }
    res = client.post(url, payload, content_type='application/json')
    assert res.status_code == 201
