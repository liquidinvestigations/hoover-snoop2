import time
from urllib.parse import urljoin

import requests
import pytest
from django.conf import settings

from snoop.data import models
from snoop.data import tasks
from snoop.data import indexing
from snoop.data import digests

from conftest import mask_out_current_collection, CollectionApiClient
from snoop.data.management.commands.filestats import get_top_mime_types, get_top_extensions

pytestmark = [pytest.mark.django_db]

ID = {
    'cheese': '2228e662341d939650d313b8971984d9'
              '9b0d50791f7b4c06034b6f254436a3c3',
    'gold': '64f585e84c751408a4b8cebf35212cbe'
            '7e3f5ea6843fed0581be212705604448',
    'easychair.docx': '36a12c77e4fd84e8d38542990f9bd657'
                      'c6afb9768cae6703fc78b37cf64e88be',
    'partialemlx': 'ed41bf32d79bc1a654b72443b73fd57f'
                   '01839ca40e4f2cfc25fddb83beb56b18',
}

SMASHED = "66a3a6bb9b8d86b7ce2be5e9f3a794a778a85fb58b8550a54b7e2821d602e1f1"


def check_api_page(api, item_id, parent_id):
    item = api.get_digest(item_id)
    page = item['parent_children_page']
    if not parent_id:
        return

    parent = api.get_digest(parent_id, page)
    assert parent['children_page'] == page
    assert parent['children_count'] > 0
    assert parent['children_page_count'] > 0
    children = list(c['id'] for c in parent['children'])
    assert item['id'] in children


@pytest.mark.django_db(transaction=True)
def test_complete_lifecycle(client, taskmanager):
    for b in settings.BLOBS_S3.list_buckets():
        bucket = b.name
        print('del bucket', bucket)
        for obj in settings.BLOBS_S3.list_objects(bucket, prefix='/', recursive=True):
            settings.BLOBS_S3.remove_object(bucket, obj.object_name)
        settings.BLOBS_S3.remove_bucket(bucket)
    settings.BLOBS_S3.make_bucket('testdata')

    models.Directory.objects.create()
    indexing.delete_index()
    indexing.create_index()

    with mask_out_current_collection():
        print('Running dispatcher')
        tasks.run_dispatcher()

    print('Running taskmanager')
    taskmanager.run(limit=90000)

    time.sleep(60)

    try:
        with mask_out_current_collection():
            print('Running bulk tasks')
            tasks.run_bulk_tasks()
    except Exception:
        print("Bulk tasks failed, trying again!")
        time.sleep(100)
        with mask_out_current_collection():
            tasks.run_bulk_tasks()

    time.sleep(10)

    print("Iterate through the feed")
    with mask_out_current_collection():
        col_url = '/collections/testdata/json'
        col = client.get(col_url).json()

        def feed_page(url):
            page = client.get(url).json()
            next_url = urljoin(url, page['next']) if page.get('next') else None
            return next_url, page['documents']

        docs = {}
        feed_url = urljoin(col_url, col['feed'])
        while feed_url:
            feed_url, page_docs = feed_page(feed_url)
            for doc in page_docs:
                docs[doc['id']] = doc

    print("Run checks...")
    # this file exists on the filesystem
    cheese = docs[ID['cheese']]
    assert cheese['content']['text'].strip() == "cheese!"

    # this file is only in a zip file, so if we find it, unzip works
    gold = docs[ID['gold']]
    assert gold['content']['text'].strip() == "gold!"

    # docx file; check that tika pulled out the text
    easychair = docs[ID['easychair.docx']]
    assert "at least 300dpi in resolution" in easychair['content']['text']

    # .partial.emlx
    partialemlx = docs[ID['partialemlx']]
    assert partialemlx['content']['subject'] == "Re: promulgare lege"

    # check that all successful digests.index tasks made it into es
    print("Check Elasticsearch")
    es_count_url = f'{settings.SNOOP_COLLECTIONS_ELASTICSEARCH_URL}/testdata/_count'
    es_count_resp = requests.get(es_count_url)
    es_count = es_count_resp.json()['count']
    db_count = models.Task.objects.filter(func='digests.index', status='success').count()
    assert es_count > 0
    assert es_count == db_count

    # check that all index ops were successful
    filtered_tasks = models.Task.objects.filter(func='digests.index')
    index_failed = [(t.args, t.status) for t in filtered_tasks.exclude(status='success')]
    assert index_failed == []

    # check that no unexpected errors happened on testdata
    assert models.Task.objects.filter(status='error').count() == 0

    # check that all files and directories are contained in their parent lists
    print("Check API page")
    api = CollectionApiClient(client)
    for f in models.File.objects.all()[:500]:
        check_api_page(api, digests.file_id(f), digests.parent_id(f))
    for d in models.Directory.objects.all()[:500]:
        if d.container_file:
            continue
        check_api_page(api, digests.directory_id(d), digests.parent_id(d))

    mime_dict_supported = get_top_mime_types(['testdata'], 300, True)
    assert 'application/pdf' in mime_dict_supported.keys()
    mime_dict_unsupported = get_top_mime_types(['testdata'], 300, False)
    assert 'application/pdf' not in mime_dict_unsupported.keys()

    ext_dict1 = get_top_extensions(['testdata'], 300, True)
    assert '.docx' in ext_dict1.keys()
    ext_dict2 = get_top_extensions(['testdata'], 300, False)
    assert '.docx' not in ext_dict2.keys()
