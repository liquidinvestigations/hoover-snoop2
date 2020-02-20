from urllib.parse import urljoin
import subprocess

import requests
import pytest
from django.conf import settings

from snoop.data import models
from snoop.data import tasks
from snoop.data import indexing

from conftest import mask_out_current_collection

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


def test_complete_lifecycle(client, taskmanager):
    blobs_path = settings.SNOOP_BLOB_STORAGE
    subprocess.check_call('rm -rf *', shell=True, cwd=blobs_path)

    models.Directory.objects.create()
    indexing.delete_index()
    indexing.create_index()

    with mask_out_current_collection():
        tasks.run_dispatcher()

    taskmanager.run(limit=10000)

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
    es_count_url = f'{settings.SNOOP_COLLECTIONS_ELASTICSEARCH_URL}/snoop2/_count'
    es_count_resp = requests.get(es_count_url)
    es_count = es_count_resp.json()['count']
    db_count = models.Task.objects.filter(func='digests.index', status='success').count()
    assert es_count > 0
    assert es_count == db_count

    # check that all index ops were successful
    filtered_tasks = models.Task.objects.filter(func='digests.index')
    index_failed = [(t.args, t.status) for t in filtered_tasks.exclude(status='success')]
    # one indexing task should be broken because
    # `encrypted-hushmail-smashed-bytes.eml` is broken
    assert index_failed == [(['66a3a6bb9b8d86b7ce2be5e9f3a794a778a85fb58b8550a54b7e2821d602e1f1'],
                             'broken')]
