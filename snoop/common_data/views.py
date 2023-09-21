"""Views for the common data."""
import json
from django.http import JsonResponse
from django.views.decorators.cache import never_cache

from . import models
from collections import defaultdict


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
