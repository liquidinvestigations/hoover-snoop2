import json
from django.http import JsonResponse
from django.shortcuts import get_object_or_404
from . import models


def zulu(t):
    txt = t.isoformat()
    assert txt.endswith('+00:00')
    return txt.replace('+00:00', 'Z')


def document_data(digest):
    with digest.result.open() as f:
        digest_data = json.loads(f.read().decode('utf8'))

    return {
        'id': digest.blob.pk,
        'version': zulu(digest.date_modified),
        'content': {
            'content-type': digest.blob.mime_type,
            'text': digest_data.get('text'),
            'md5': digest.blob.md5,
            'sha1': digest.blob.sha1,
            'size': digest.blob.path().stat().st_size,
        },
    }


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
    return JsonResponse(document_data(digest))
