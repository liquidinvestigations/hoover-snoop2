"""Views for the common data."""
import json
import logging
from django.http import JsonResponse, HttpResponse
from django.views.decorators.cache import never_cache

from . import models
from snoop.data import collections
from collections import defaultdict

logger = logging.getLogger(__name__)


@never_cache
def get_collection_hits(request):
    """Look for duplicates in a fixed collection set."""

    MAX_COLLECTION_COUNT = 100
    MAX_DOC_HASH_COUNT = 10000
    body = json.loads(request.body.decode('utf-8'))
    collections = body['collection_list']
    doc_ids = body['doc_sha3_list']
    assert collections
    assert doc_ids
    assert len(collections) <= MAX_COLLECTION_COUNT
    assert len(doc_ids) <= MAX_DOC_HASH_COUNT
    max_result_count = len(collections) * len(doc_ids)

    queryset = (
        models.CollectionDocumentHit.objects
        .filter(
            doc_sha3_256__in=doc_ids,
            collection_name__in=collections,
        )
        .order_by('-doc_date_added')
    )[:max_result_count]
    hits = defaultdict(list)
    for hit in queryset:
        hits[hit.doc_sha3_256].append(hit.collection_name)

    return JsonResponse({"hits": hits})


def get_nextcloud_collections(request):
    """View that returns names of nextcloud collections."""
    nextcloud_collections = models.NextcloudCollection.objects.all()
    result = {'nextcloud_collections': [{'name': nc_col.name} for nc_col in nextcloud_collections]}
    return JsonResponse(result)


def sync_nextlcoud_collections(request):
    """View that syncs nextcloud collections with hoover search.

    Receives a JSON with all the nextcloud collections that are set up
    in hoover search and syncs it with the collections that are already
    registered in snoop.
    """
    if request.method == 'POST':
        nc_collections = json.loads(request.body)
        for nc_col in nc_collections:
            col_name = nc_col.get('name')
            _, created = models.NextcloudCollection.objects.update_or_create(
                name=col_name, defaults={"opt": nc_col}
            )

            if not created:
                logger.info(f'Updated collection {col_name}.')
                continue

            logger.info(f'Created collection {col_name}.')
            collections.create_databases()
            logger.info(f'Created databases for: {col_name}.')
            collections.migrate_databases()
            logger.info(f'Migrated databases for: {col_name}.')
            collections.create_es_indexes()
            logger.info(f'Created es indices for: {col_name}.')
            collections.create_roots()
            logger.info(f'Created roots for: {col_name}.')
        return HttpResponse(status=200)
