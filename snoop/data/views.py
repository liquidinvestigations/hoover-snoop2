import json
from django.http import JsonResponse
from django.shortcuts import get_object_or_404
from . import models
from . import digests


def collection(request, name):
    collection = get_object_or_404(models.Collection.objects, name=name)
    return JsonResponse({
        'name': name,
        'title': name,
        'description': name,
        'feed': 'feed',
        'data_urls': '{id}/json',
    })


def feed(request, name):
    collection = get_object_or_404(models.Collection.objects, name=name)
    query = collection.digest_set.order_by('-date_modified')

    return JsonResponse({
        'documents': [digests.get_document_data(d) for d in query],
    })


def document(request, name, hash):
    collection = get_object_or_404(models.Collection.objects, name=name)
    digest = get_object_or_404(collection.digest_set, blob__pk=hash)
    return JsonResponse(digests.get_document_data(digest))
