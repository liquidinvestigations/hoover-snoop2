import pytest
from snoop.data import collections, models
from conftest import TESTDATA
from django.conf import settings
import requests
import time

ES_URL = settings.SNOOP_COLLECTIONS_ELASTICSEARCH_URL

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

    res = query_es_tag('test-tag').json()

    # polling for tags
    start = time.time()
    while not res['hits']['hits']:
        if time.time() - start >= 300:
            raise Exception('Indexing tags timed out!')
        time.sleep(1)
        res = query_es_tag('test-tag').json()

    tags = res['hits']['hits'][0]['_source']['tags']
    assert 'test-tag' in tags


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


def query_es_tag(tag):
    """Query elasticsearch for tag and return response."""
    es_index = collections.current().es_index
    url = f'{ES_URL}/{es_index}/_search'
    query = {"query": {
        "query_string": {
            "query": tag,
            "default_field": "tags"
        }
    }}
    res = requests.get(url=url, headers={'Content-Type': 'application/json'}, json=query)
    assert res.status_code == 200
    return res
