from pathlib import Path
from urllib.parse import urljoin
import pytest
from django.conf import settings
from snoop.data.tasks import shaorma
from snoop.data import models
from snoop.data import dispatcher

pytestmark = [pytest.mark.django_db]

ID = {
    'cheese': '2228e662341d939650d313b8971984d9'
              '9b0d50791f7b4c06034b6f254436a3c3',
    'gold': '64f585e84c751408a4b8cebf35212cbe'
            '7e3f5ea6843fed0581be212705604448',
}


def test_walk_and_api(client):
    col = models.Collection.objects.create(
        name='testdata',
        root=Path(settings.SNOOP_TESTDATA) / 'data',
    )
    root = col.directory_set.create()

    dispatcher.run_dispatcher()

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