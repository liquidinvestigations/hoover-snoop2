"""Django views, mostly JSON APIs.
"""
from functools import wraps
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

TEXT_LIMIT = 10 ** 6  # one million characters


def collection_view(func):
    """Decorator for views Django bound to a collection.

    The collection slug is set through an URL path parameter called "collection".
    """

    @wraps(func)
    def view(request, *args, collection, **kwargs):
        try:
            col = collections.ALL[collection]
        except KeyError:
            raise Http404(f"Collection {collection} does not exist")

        with col.set_current():
            return func(request, *args, **kwargs)

    return view


def drf_collection_view(func):
    """Decorator for Django Rest Framework viewset methods bound to a collection.

    The collection slug is set through the `kwargs` field on the `rest_framework.viewsets.ModelViewSet`
    called "collection". The `kwargs` are set by Django Rest Framework from the URL path parameter, so
    result is similar to `snoop.data.views.collection_view() defined above`.
    """

    @wraps(func)
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
    """View returns basic stats for a collection as JSON.

    Also loads the "stats" for this collection, as saved by `snoop.data.admin.get_stats`.
    """

    col = collections.current()
    stats, _ = models.Statistics.objects.get_or_create(key='stats')
    return JsonResponse({
        'name': col.name,
        'title': col.name,
        'description': col.name,
        'feed': 'feed',
        'data_urls': '{id}/json',
        'stats': stats.value,
        'max_result_window': col.max_result_window,
        'refresh_interval': col.refresh_interval,
    })


@collection_view
def feed(request):
    """JSON view used to paginate through entire Digest database, sorted by last modification date.

    This was used in the past by another service to pull documents as they are processed and index them
    elsewhere. This is not used anymore by us, since we now index documents in a snoop Task. See
    `snoop.data.digests.index` for the Task definition.

    TODO: deprecate and remove this view.
    """
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
    """JSON view with data for a File.

    The primary key of the File is used to fetch it.

    Response is different from, but very similar to, the result of the `document()` view below.
    """

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
    """JSON view with data for a Digest.

    The primary key of the Digest is used to fetch it.

    These are the de-duplicated variants of the objects returned from `file_view()` above, with some
    differences. See `snoop.data.digests.get_document_data()` versus `snoop.data.digests.get_file_data()`.
    """

    digest = get_object_or_404(models.Digest.objects, blob__pk=hash)
    children_page = int(request.GET.get('children_page', 1))
    return JsonResponse(trim_text(digests.get_document_data(digest, children_page)))


@collection_view
def document_download(request, hash, filename):
    """View to download the `.original` Blob for the first File in a Digest's set.

    Since all post-conversion `.blob`s are bound to the same `Digest` object, we assume the `.original`
    Blobs are all equal too; so we present only the first one for downloading. This might cause problems
    when this does not happen for various reasons; since users can't actually download all the different
    original versions present in the dataset.

    In practice, the conversion tools we use generally produce
    different results every time they're run on the same file, so the chance of this happening are
    non-existant. This also means we don't de-duplicate properly for files that require conversion.
    See `snoop.data.filesystem.handle_file()` for more details.
    """

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
    """View to download the OCR result binary for a given Document and OCR source combination.

    The file downloaded can either be a PDF document with selectable text imprinted in it, or a text file.

    The OCR source can be either External OCR (added by management command
    `snoop.data.management.commands.createocrsource` or through the Admin), or managed internally (with the
    slug called `tesseract_$LANG`).

    The given slug "ocrname" is first looked up in the `snoop.data.models.OcrSource` table. If it's not
    there, then we look in the Tasks table for dependencies of this document's Digest task, and return the
    one with name matching the slug.
    """

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
    """JSON view to paginate through all locations for a Digest.

    Used to browse between the different apparitions of a File in a dataset.

    Paginated by integers with fixed length pages, starting from 1.
    """

    digest = get_object_or_404(models.Digest.objects, blob__pk=hash)
    page = int(request.GET.get('page', 1))
    locations, has_next = digests.get_document_locations(digest, page)
    return JsonResponse({'locations': locations, 'page': page, 'has_next_page': has_next})


class TagViewSet(viewsets.ModelViewSet):
    """Django Rest Framework (DRF) View set for the Tags APIs.

    This is responsible for: capturing the various URL path arguments as the viewset context; setting the
    current collection with `drf_collection_view()`; restricting private Tags access to correct users.
    """

    serializer_class = serializers.DocumentUserTagSerializer
    permission_classes = []

    @drf_collection_view
    def get_serializer(self, *args, **kwargs):
        """Set a context with the path arguments.

        Generates fake values when instantiated by Swagger.
        """
        fake = getattr(self, 'swagger_fake_view', False)
        if fake:
            context = {
                'collection': "some-collection",
                'blob': "0006660000000000000000000000000000000000000000000000000000000000",
                'user': "testuser",
                'digest_id': 666,
                'uuid': 'invalid',
            }
        else:
            context = {
                'collection': self.kwargs['collection'],
                'blob': self.kwargs['hash'],
                'user': self.kwargs['username'],
                'digest_id': models.Digest.objects.filter(blob=self.kwargs['hash']).get().id,
                'uuid': self.kwargs['uuid'],
            }
        return super().get_serializer(*args, **kwargs, context=context)

    @drf_collection_view
    def dispatch(self, *args, **kwargs):
        """Collection-aware overload."""
        return super().dispatch(*args, **kwargs)

    @drf_collection_view
    def get_queryset(self):
        """Sets this TagViewSet's queryset to tags that are private to the current user, or that are public.
        """

        user = self.kwargs['username']
        blob = self.kwargs['hash']
        assert models.Digest.objects.filter(blob=blob).exists(), 'hash is not digest'
        return models.DocumentUserTag.objects.filter(Q(user=user) | Q(public=True), Q(digest__blob=blob))

    def check_ownership(self, pk):
        """Raises error if tag does not belong to current user.

        To be used when doing write operations.
        """
        assert self.kwargs['username'] == self.get_queryset().get(pk=pk).user, \
            "you can only modify your own tags"

    @drf_collection_view
    def update(self, request, pk=None, **kwargs):
        """Collection-aware overload that also checks permission to write tag."""
        self.check_ownership(pk)
        return super().update(request, pk, **kwargs)

    @drf_collection_view
    def partial_update(self, request, pk=None, **kwargs):
        """Collection-aware overload that also checks permission to write tag."""
        self.check_ownership(pk)
        return super().partial_update(request, pk, **kwargs)

    @drf_collection_view
    def destroy(self, request, pk=None, **kwargs):
        """Collection-aware overload that also checks permission to write tag."""
        self.check_ownership(pk)
        return super().destroy(request, pk, **kwargs)


@collection_view
def thumbnail(request, hash, size):
    thumbnail_entry = get_object_or_404(models.Thumbnail.objects, size=size, blob__pk=hash)
    return FileResponse(thumbnail_entry.thumbnail.open(), content_type='image/jpeg')
