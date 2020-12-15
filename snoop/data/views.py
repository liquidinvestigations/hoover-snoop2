from django.http import HttpResponse, JsonResponse, FileResponse, Http404
from django.shortcuts import get_object_or_404
from django.conf import settings
from rest_framework import viewsets
from . import models
from . import digests
from . import ocr
from . import collections
from . import serializers
from .analyzers import html
from django.db.models import Q

TEXT_LIMIT = 10 ** 7  # ten million characters


def collection_view(func):
    def view(request, *args, collection, **kwargs):
        try:
            col = collections.ALL[collection]
        except KeyError:
            raise Http404(f"Collection {collection} does not exist")

        with col.set_current():
            return func(request, *args, **kwargs)

    return view


def drf_collection_view(func):
    def view(self, *args, **kwargs):
        try:
            collection = self.kwargs['collection']
            col = collections.ALL[collection]
        except KeyError:
            raise Http404("Collection does not exist")

        with col.set_current():
            return func(self, *args, **kwargs)

    return view


@collection_view
def collection(request):
    col = collections.current()
    stats, _ = models.Statistics.objects.get_or_create(key='stats')
    return JsonResponse({
        'name': col.name,
        'title': col.name,
        'description': col.name,
        'feed': 'feed',
        'data_urls': '{id}/json',
        'stats': stats.value,
    })


@collection_view
def feed(request):
    limit = settings.SNOOP_FEED_PAGE_SIZE
    query = models.Digest.objects.order_by('-date_modified')

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


@collection_view
def file_view(request, pk):
    file = get_object_or_404(models.File.objects, pk=pk)
    children_page = int(request.GET.get('children_page', 1))
    return JsonResponse(trim_text(digests.get_file_data(file, children_page)))


@collection_view
def directory(request, pk):
    directory = get_object_or_404(models.Directory.objects, pk=pk)
    children_page = int(request.GET.get('children_page', 1))
    return JsonResponse(digests.get_directory_data(directory, children_page))


def trim_text(data):
    """ Trim the text fields to TEXT_LIMIT chars """
    if not data.get('content'):
        return data

    text = data['content'].get('text')

    # For images and the like, text is None.
    if not text:
        return data

    if len(text) > TEXT_LIMIT:
        text = text[:TEXT_LIMIT] + "\n\n=== Long text trimmed by Hoover ===\n"
    data['content']['text'] = text
    return data


@collection_view
def document(request, hash):
    digest = get_object_or_404(models.Digest.objects, blob__pk=hash)
    children_page = int(request.GET.get('children_page', 1))
    return JsonResponse(trim_text(digests.get_document_data(digest, children_page)))


@collection_view
def document_download(request, hash, filename):
    digest = get_object_or_404(
        models.Digest.objects.only('blob'),
        blob__pk=hash,
    )
    blob = digest.blob.file_set.first().original

    if html.is_html(blob):
        clean_html = html.clean(blob)
        return HttpResponse(clean_html, content_type='text/html')

    return FileResponse(blob.open(), content_type=blob.content_type)


@collection_view
def document_ocr(request, hash, ocrname):
    digest = get_object_or_404(models.Digest.objects, blob__pk=hash)

    if models.OcrSource.objects.filter(name=ocrname).exists():
        # serve file from external OCR import
        ocr_source = get_object_or_404(models.OcrSource, name=ocrname)
        ocr_queryset = ocr.ocr_documents_for_blob(digest.blob)
        ocr_document = get_object_or_404(ocr_queryset, source=ocr_source)

        blob = ocr_document.ocr
    else:
        digest_task = get_object_or_404(models.Task.objects, func='digests.gather', args=[hash])
        tesseract_task = digest_task.prev_set.get(name=ocrname).prev
        blob = tesseract_task.result
    return FileResponse(blob.open(), content_type=blob.content_type)


@collection_view
def document_locations(request, hash):
    digest = get_object_or_404(models.Digest.objects, blob__pk=hash)
    page = int(request.GET.get('page', 1))
    locations, has_next = digests.get_document_locations(digest, page)
    return JsonResponse({'locations': locations, 'page': page, 'has_next_page': has_next})


class TagViewSet(viewsets.ModelViewSet):
    serializer_class = serializers.DocumentUserTagSerializer
    permission_classes = []

    @drf_collection_view
    def get_serializer(self, *args, **kwargs):
        fake = getattr(self, 'swagger_fake_view', False)
        if fake:
            context = {
                'collection': "some-collection",
                'blob': "0006660000000000000000000000000000000000000000000000000000000000",
                'user': "testuser",
                'digest_id': 666,
            }
        else:
            context = {
                'collection': self.kwargs['collection'],
                'blob': self.kwargs['hash'],
                'user': self.kwargs['username'],
                'digest_id': models.Digest.objects.filter(blob=self.kwargs['hash']).get().id,
            }
        return super().get_serializer(*args, **kwargs, context=context)

    @drf_collection_view
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)

    @drf_collection_view
    def get_queryset(self):
        user = self.kwargs['username']
        blob = self.kwargs['hash']
        assert models.Digest.objects.filter(blob=blob).exists(), 'hash is not digest'
        return models.DocumentUserTag.objects.filter(Q(user=user) | Q(public=True), Q(digest__blob=blob))

#    @drf_collection_view
#    def create(self, request, **kwargs):
#        log.error('create request kwargs ' + str(self.kwargs))
#        log.error('function kwargs ' + str(kwargs))
#
#        blob = self.kwargs['hash']
#        user = self.kwargs['username']
#        digest_id = models.Digest.objects.get(blob=blob).id
#        return super().create(request)
#
#    @drf_collection_view
#    def update(self, request, pk=None):
#        blob = self.kwargs['hash']
#        request.data['user'] = self.kwargs['username']
#        request.data['digest_id'] = models.Digest.objects.get(blob=blob).id
#        return super().update(request, pk)
#
#    @drf_collection_view
#    def partial_update(self, request, pk=None):
#        blob = self.kwargs['hash']
#        request.data['user'] = self.kwargs['username']
#        request.data['digest_id'] = models.Digest.objects.get(blob=blob).id
#        return super().partial_update(request, pk)
#
#    @drf_collection_view
#    def destroy(self, request, pk=None):
#        blob = self.kwargs['hash']
#        request.data['user'] = self.kwargs['username']
#        request.data['digest_id'] = models.Digest.objects.get(blob=blob).id
#        return super().destroy(request, pk)
