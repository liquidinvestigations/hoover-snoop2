import json
from django.http import JsonResponse
from django.shortcuts import get_object_or_404
from . import models


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
    return JsonResponse({
        'documents': [],
    })


def document(request, name, hash):
    collection = get_object_or_404(models.Collection.objects, name=name)
    digest = get_object_or_404(collection.digest_set, blob__pk=hash)

    with digest.result.open() as f:
        digest_data = json.loads(f.read().decode('utf8'))

    return JsonResponse({
        'content': {
            'text': digest_data.get('text'),
        },
    })
