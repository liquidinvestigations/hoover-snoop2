import pytest
import csv
from snoop.data import collections, models
from conftest import TESTDATA
from django.conf import settings
from django.core.management import call_command
import requests
import time

ES_URL = settings.SNOOP_COLLECTIONS_ELASTICSEARCH_URL

pytestmark = [pytest.mark.django_db]


def test_tags_api(fakedata, taskmanager, client, django_user_model):
    root = fakedata.init()

    testfile1 = (TESTDATA / './no-extension/file_pdf')
    testfile2 = (TESTDATA / './disk-files/images/bikes.jpg')

    with testfile1.open('rb') as f:
        blob1 = fakedata.blob(f.read())

    with testfile2.open('rb') as f:
        blob2 = fakedata.blob(f.read())

    fakedata.file(root, 'file1.pdf', blob1)
    fakedata.file(root, 'file2.jpg', blob2)

    taskmanager.run(limit=3000)

    tags_mapping = {
        blob1.md5: ['tag1', 'tag2'],
        blob2.md5: ['tag1', 'tag3']
    }
    create_tags_csv(tags_mapping, '/tmp/tags.csv')
    col = collections.current().name
    user = django_user_model.objects.create_user(username='test', password='pw')
    call_command('importtags', col=col, tags='/tmp/tags.csv', user=user.username, uuid='xyz-123', p=True)
    digest1 = models.Digest.objects.get(blob_id=blob1.pk)
    digest2 = models.Digest.objects.get(blob_id=blob2.pk)

    assert models.DocumentUserTag.objects.filter(digest_id=digest1.pk, tag='tag1').exists()
    assert models.DocumentUserTag.objects.filter(digest_id=digest1.pk, tag='tag2').exists()
    assert models.DocumentUserTag.objects.filter(digest_id=digest2.pk, tag='tag1').exists()
    assert models.DocumentUserTag.objects.filter(digest_id=digest2.pk, tag='tag3').exists()

    # need to wait for the document to be indexed
    time.sleep(5)

    res1 = query_es_tag('tag2')
    tags1 = res1.json()['hits']['hits'][0]['_source']['tags']
    assert res1.status_code == 200
    assert 'tag1' in tags1 and 'tag2' in tags1

    res2 = query_es_tag('tag3')
    tags2 = res2.json()['hits']['hits'][0]['_source']['tags']
    assert res2.status_code == 200
    assert 'tag1' in tags2 and 'tag3' in tags2


def create_tags_csv(tags_mapping, csv_path):
    '''Create a csv with tags to import.

    Takes a dictionary in the form of {blob: [tag1, tag2]}
    as input.
    '''
    header = ['MD5 Hash', 'Tags']
    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for blob in tags_mapping:
            # create a list to write to the file
            # the format is [blob_hash, 'tag1, tag2']
            data = [blob] + [', '.join(tags_mapping[blob])]
            writer.writerow(data)


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
    return requests.get(url=url, headers={'Content-Type': 'application/json'}, json=query)
