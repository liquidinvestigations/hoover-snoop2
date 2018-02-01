import json
from django.http import HttpResponse, JsonResponse, FileResponse
from django.shortcuts import get_object_or_404
from django.conf import settings
from . import models
from . import digests
from .analyzers import html


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

    limit = settings.SNOOP_FEED_PAGE_SIZE
    query = collection.digest_set.order_by('-date_modified')

    lt = request.GET.get('lt')
    if lt:
        query = query.filter(date_modified__lt=lt)

    documents = [digests.get_document_data(d) for d in query[:limit]]

    if len(documents) < limit:
        next_page = None

    else:
        last_version = documents[-1]['version']
        next_page = f'?lt={last_version}'

    return JsonResponse({
        'documents': documents,
        'next': next_page,
    })


def directory(request, name, pk):
    collection = get_object_or_404(models.Collection.objects, name=name)
    directory = get_object_or_404(collection.directory_set, pk=pk)
    return JsonResponse(digests.get_directory_data(directory))


def document(request, name, hash):
    collection = get_object_or_404(models.Collection.objects, name=name)
    digest = get_object_or_404(collection.digest_set, blob__pk=hash)
    return JsonResponse(digests.get_document_data(digest))


def document_download(request, name, hash, filename):
    collection = get_object_or_404(models.Collection.objects, name=name)
    digest = get_object_or_404(collection.digest_set, blob__pk=hash)
    blob = digest.blob

    if html.is_html(blob):
        clean_html = html.clean(blob)
        return HttpResponse(clean_html, content_type='text/html')

    return FileResponse(digest.blob.open(), content_type=blob.content_type)


def document_locations(request, name, hash):
    collection = get_object_or_404(models.Collection.objects, name=name)
    digest = get_object_or_404(collection.digest_set, blob__pk=hash)
    locations = digests.get_document_locations(digest)
    return JsonResponse({'locations': locations})
