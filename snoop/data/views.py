"""Django views, mostly JSON APIs.
"""
from functools import wraps
import logging
from hashlib import sha1

from django.conf import settings
from django.db.models import Q
from django.http import FileResponse, Http404, HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404
from ranged_response import RangedFileResponse
from rest_framework import viewsets
from django.views.decorators.cache import cache_control, never_cache
from django.views.decorators.http import condition
from django.views.decorators.vary import vary_on_headers
from django.core.cache import caches as django_caches
from django.db.models import Max

from . import collections, digests, models, ocr, serializers, tracing
from .tasks import dispatch_directory_walk_tasks
from .analyzers import html

from snoop.data.pdf_tools import split_pdf_file, get_pdf_info, pdf_extract_text


TEXT_LIMIT = 10 ** 6  # one million characters
tracer = tracing.Tracer(__name__)
log = logging.getLogger(__name__)
CACHE_VARY_ON_HEADERS = ['Range']  # don't vary by Cookie, it's fake

# cache all 'downloadable' documents on server for a week - these will never change
DOWNLOAD_CACHE_MAX_AGE = 7 * 24 * 3600

# cache settings for data expected to change, e.g. new file uploaded to directory,
# or document data after a new tag. These can be re-evaludated every 1-2min,
# to decrease load.
MEDIUM_CACHE_OPTIONS = dict(
    private=True,
    max_age=0,
    must_revalidate=True,
)

# for views we never expect to change, e.g. get path for directory ID.
# revalidate every 5-10min
LONG_CACHE_OPTIONS = dict(
    private=True,
    max_age=0,
    must_revalidate=True,
)

MAX_CACHE_ITEM_SIZE = 150 * 2**20  # many MB

# revalidate every 1min, for things that can lag behind, e.g. collection stats,
# task processing status - useful to decrease load
SHORT_LIVED_CACHE_OPTIONS = dict(
    private=True,
    max_age=0,
    must_revalidate=True,
)


# these will revalidate every time, useful when we have ETAG/Modified,
# but we expect them to change often (e.g. doc/json). Used with `@condition`
def collection_view(func):
    """Decorator for views Django bound to a collection.

    The collection slug is set through an URL path parameter called "collection".
    """

    @tracer.wrap_function()
    @wraps(func)
    def view(request, *args, collection, **kwargs):
        try:
            col = collections.ALL[collection]
        except KeyError:
            raise Http404(f"Collection {collection} does not exist")

        with col.set_current():
            tracer.count('api_collection_view')
            return func(request, *args, **kwargs)

    return view


def drf_collection_view(func):
    """Decorator for Django Rest Framework viewset methods bound to a collection.

    The collection slug is set through the `kwargs` field on the `rest_framework.viewsets.ModelViewSet`
    called "collection". The `kwargs` are set by Django Rest Framework from the URL path parameter, so
    result is similar to `snoop.data.views.collection_view() defined above`.
    """

    @tracer.wrap_function()
    @wraps(func)
    def view(self, *args, **kwargs):
        try:
            collection = self.kwargs['collection']
            col = collections.ALL[collection]
        except KeyError:
            raise Http404("Collection does not exist")

        with col.set_current():
            tracer.count('api_collection_view')
            return func(self, *args, **kwargs)

    return view


@collection_view
@cache_control(**SHORT_LIVED_CACHE_OPTIONS)
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
        'stats': {k: v for k, v in stats.value.items() if not k.startswith('_')},
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

    documents = [digests.get_document_data(d.blob) for d in query[:limit]]

    if len(documents) < limit:
        next_page = None

    else:
        last_version = documents[-1]['version']
        next_page = f'?lt={last_version}'

    return JsonResponse({
        'documents': documents,
        'next': next_page,
    })


def file_digest_last_modified(request, pk, *_args, **_kw):
    """Get the last modified ts of either this File obj or any of its children"""
    file = get_object_or_404(models.File.objects, pk=pk)
    try:
        doc_ts = file.blob.digest.date_modified
    except models.Blob.digest.RelatedObjectDoesNotExist:
        doc_ts = file.date_modified

    if file.child_directory_set.exists():
        children_ts = file.child_directory_set.aggregate(maxval=Max('date_modified'))['maxval']
        doc_ts = max(children_ts, doc_ts)

    return doc_ts


@collection_view
@cache_control(**MEDIUM_CACHE_OPTIONS)
@condition(last_modified_func=file_digest_last_modified)
def file_view(request, pk):
    """JSON view with data for a File.

    The primary key of the File is used to fetch it.

    Response is different from, but very similar to, the result of the `document()` view below.
    """

    file = get_object_or_404(models.File.objects, pk=pk)
    children_page = int(request.GET.get('children_page', 1))
    return JsonResponse(trim_text(digests.get_file_data(file, children_page)))


def directory_last_modified(request, pk, *_args, **_kw):
    """Get the last modified ts of either this Dir obj or any of its children"""
    directory = get_object_or_404(models.Directory.objects, pk=pk)
    doc_ts = directory.date_modified
    if directory.child_directory_set.exists():
        children_ts = directory.child_directory_set.aggregate(maxval=Max('date_modified'))['maxval']
        doc_ts = max(children_ts, doc_ts)
    if directory.child_file_set.exists():
        children_ts = directory.child_file_set.aggregate(maxval=Max('date_modified'))['maxval']
        doc_ts = max(children_ts, doc_ts)
    return doc_ts


@collection_view
@cache_control(**MEDIUM_CACHE_OPTIONS)
@condition(last_modified_func=directory_last_modified)
def directory(request, pk):
    directory = get_object_or_404(models.Directory.objects, pk=pk)
    children_page = int(request.GET.get('children_page', 1))
    return JsonResponse(digests.get_directory_data(directory, children_page))


@collection_view
@cache_control(**MEDIUM_CACHE_OPTIONS)
@condition(last_modified_func=directory_last_modified)
def file_exists(request, directory_pk, filename):
    """View that checks if a given file exists in the database.

    Args:
        directory_pk: Primary key of the files parent directory.
        filename: String that is the name of the file.

    Returns:
        A HTTP response containing the primary key (hash) of the original blob,
        if the file exists.

        A HTTP 404 response if the file doesn't exist.
    """
    try:
        file = models.File.objects.get(
            name_bytes=str.encode(filename),
            parent_directory__pk=directory_pk)
    except models.File.DoesNotExist:
        return HttpResponse(status=404)
    if file:
        return HttpResponse(file.original.pk)


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


def document_digest_last_modified(request, hash, *_args, **_kw):
    digest = get_object_or_404(models.Digest.objects, blob__pk=hash)
    return digest.date_modified


def document_digest_etag_key(request, hash, *_args, **_kw):
    digest = get_object_or_404(models.Digest.objects, blob__pk=hash)
    return digest.get_etag()


@collection_view
@cache_control(**MEDIUM_CACHE_OPTIONS)
@condition(last_modified_func=document_digest_last_modified, etag_func=document_digest_etag_key)
def document(request, hash):
    """JSON view with data for a Digest.

    The hash of the Digest source object is used to fetch it. If a Digest object doesn't exist, that means
    processing has failed and we need to fetch the File for metadata.

    These are the de-duplicated variants of the objects returned from `file_view()` above, with some
    differences. See `snoop.data.digests.get_document_data()` versus `snoop.data.digests.get_file_data()`.
    """

    blob = models.Blob.objects.get(pk=hash)
    children_page = int(request.GET.get('children_page', 1))
    return JsonResponse(trim_text(digests.get_document_data(blob, children_page)))


def _apply_pdf_tools(request, blob):
    """Split PDF files into parts depending on GET options"""
    HEADER_RANGE = 'X-Hoover-PDF-Split-Page-Range'
    HEADER_PDF_INFO = 'X-Hoover-PDF-Info'
    HEADER_PDF_EXTRACT_TEXT = 'X-Hoover-PDF-Extract-Text'
    HEADER_PDF_EXTRACT_TEXT_METHOD = 'X-Hoover-PDF-Extract-Text-Method'

    # pass over unrelated requests
    _get_info = request.GET.get(HEADER_PDF_INFO, '')
    _get_range = request.GET.get(HEADER_RANGE, '')
    _get_text = request.GET.get(HEADER_PDF_EXTRACT_TEXT, '')
    _get_text_method = request.GET.get(HEADER_PDF_EXTRACT_TEXT_METHOD, '')

    if (
        request.method != 'GET'
        or not (
            _get_info or _get_range or _get_text
        )
    ):
        log.warning('ignoring pdf tools...')
        return None

    if request.headers.get('Range'):
        log.warning('PDF Tools: Reject Range query')
        return HttpResponse('X-Hoover-PDF does not work with HTTP-Range', status=400)

    def _read_chunks(path):
        with open(path, 'rb') as f:
            while chunk := f.read(16 * 1024):
                yield chunk

    content_type = 'application/pdf'
    streaming_content = None
    with blob.mount_path() as blob_path:
        if _get_info:
            streaming_content = get_pdf_info(blob_path)
            content_type = 'application/json'
        else:
            if _get_range:
                # parse the range to make sure it's 1-100 and not some bash injection
                page_start, page_end = _get_range.split('-')
                page_start, page_end = int(page_start), int(page_end)
                assert 0 < page_start <= page_end, 'bad page interval'
                assert 0 <= page_end - page_start <= 7000, 'too many pages'
                _range = f'{page_start}-{page_end}'
                streaming_content = split_pdf_file(blob_path, _range)

            if _get_text:
                if not streaming_content:
                    streaming_content = _read_chunks(blob_path)
                streaming_content = pdf_extract_text(streaming_content, _get_text_method)
                content_type = 'text/plain; charset=utf-8'

        # load up response in memory because we want to cache this
        content = b''.join(streaming_content)
        response = HttpResponse(content, status=200)
        response['Content-Type'] = content_type
        response[HEADER_PDF_INFO] = _get_info
        response[HEADER_RANGE] = _get_range
        response[HEADER_PDF_EXTRACT_TEXT] = _get_text
        response[HEADER_PDF_EXTRACT_TEXT_METHOD] = _get_text_method
        return response


def _cache_small_streaming(func):
    """Decorator for caching all non-streaming http view in the database,
    leaving untouched any streaming responses.
    """
    cache = django_caches['small_download_cache']
    CACHE_VERSION = '4'

    @wraps(func)
    def view(request, blob, *args, **kw):
        cache_key = (
            CACHE_VERSION + ':: ' + str(blob.pk)
            + '::' + str(request.get_full_path())
            + '::' + ','.join(
                request.headers.get(h, '')
                for h in CACHE_VARY_ON_HEADERS
            )
        )
        # stop django complaining about memcache not accepting long keys
        cache_key = cache_key.encode('utf-8', errors='backslashreplace')
        cache_key = sha1(cache_key).hexdigest()
        if resp := cache.get(cache_key):
            log.warning('CACHE HIT size %s: %s', len(resp.content), cache_key)
            return resp
        resp = func(request, blob, *args, **kw)
        if (
            not resp.streaming
            and 0 < len(resp.content) <= MAX_CACHE_ITEM_SIZE
            and 200 <= resp.status_code < 300
        ):
            log.warning('CACHE ADD size %s: %s', len(resp.content), cache_key)
            cache.add(cache_key, resp, timeout=DOWNLOAD_CACHE_MAX_AGE)
        else:
            log.warning('CACHE REJECT (streaming, non-200 or bad size): %s', cache_key)
        return resp

    return view


@_cache_small_streaming
def _get_http_response_for_blob(request, blob, filename=None):
    """Return a streaming response that reads this blob,
    respecting Range headers and any special GET args.

    Large responses are streaming, while small ones are loaded in
    memory to be cached.
    """

    if _pdf_tools_resp := _apply_pdf_tools(request, blob):
        return _pdf_tools_resp

    def _set_headers(response, filename=None):
        response['Accept-Ranges'] = 'bytes'
        response['Content-Type'] = blob.content_type
        if filename:
            response['Content-Disposition'] = f'attachment; filename="{filename}"'
        return response

    if 'HTTP_RANGE' in request.META:
        with blob.open(need_seek=True, need_fileno=True) as f:
            response = RangedFileResponse(request, f, content_type=blob.content_type)
            response = _set_headers(response)
            # if small chunk, read it all up to be cached
            if int(response['Content-Length']) < MAX_CACHE_ITEM_SIZE:
                content = b''.join(response.streaming_content)
                return HttpResponse(
                    content,
                    headers=response.headers,
                    status=response.status_code,
                )
            return response

    # for small blobs, and no Range query, load in memory and return
    if blob.size < MAX_CACHE_ITEM_SIZE:
        with blob.open() as f:
            content = f.read()
            response = HttpResponse(content)
            return _set_headers(response, filename)

    # for big ones, just stream the thing back
    with blob.open(need_seek=True, need_fileno=True) as f:
        response = FileResponse(f)
        return _set_headers(response, filename)


@collection_view
@vary_on_headers(*CACHE_VARY_ON_HEADERS)
@cache_control(**LONG_CACHE_OPTIONS)
@condition(last_modified_func=document_digest_last_modified, etag_func=document_digest_etag_key)
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
    first_file = digest.blob.file_set.first()
    blob = first_file.original

    if html.is_html(blob):
        clean_html = html.clean(blob)
        return HttpResponse(clean_html, content_type='text/html')

    real_filename = first_file.name_bytes.tobytes().decode('utf-8', errors='replace')
    real_filename = real_filename.replace("\r", "").replace("\n", "")

    return _get_http_response_for_blob(request, blob, real_filename)


@collection_view
@vary_on_headers(*CACHE_VARY_ON_HEADERS)
@cache_control(**LONG_CACHE_OPTIONS)
@condition(last_modified_func=document_digest_last_modified, etag_func=document_digest_etag_key)
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

    return _get_http_response_for_blob(request, blob)


@collection_view
@cache_control(**MEDIUM_CACHE_OPTIONS)
@condition(last_modified_func=document_digest_last_modified, etag_func=document_digest_etag_key)
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
            try:
                digest_id = models.Digest.objects.filter(blob=self.kwargs['hash']).get().id
            except models.Digest.DoesNotExist:
                digest_id = None

            context = {
                'collection': self.kwargs['collection'],
                'blob': self.kwargs['hash'],
                'user': self.kwargs['username'],
                'digest_id': digest_id,
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

        # let queryset return empty list
        # assert models.Digest.objects.filter(blob=blob).exists(), 'hash is not digest'

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
@vary_on_headers(*CACHE_VARY_ON_HEADERS)
@cache_control(**LONG_CACHE_OPTIONS)
@condition(last_modified_func=document_digest_last_modified, etag_func=document_digest_etag_key)
def thumbnail(request, hash, size):
    blob = get_object_or_404(models.Thumbnail.objects, size=size, blob__pk=hash).thumbnail
    return _get_http_response_for_blob(request, blob)


@collection_view
@vary_on_headers(*CACHE_VARY_ON_HEADERS)
@cache_control(**LONG_CACHE_OPTIONS)
@condition(last_modified_func=document_digest_last_modified, etag_func=document_digest_etag_key)
def pdf_preview(request, hash):
    blob = get_object_or_404(models.PdfPreview.objects, blob__pk=hash).pdf_preview
    return _get_http_response_for_blob(request, blob)


@collection_view
@cache_control(**LONG_CACHE_OPTIONS)
def get_path(request, directory_pk):
    """Get the full path of a given directory"""
    directory = models.Directory.objects.get(pk=directory_pk)
    # check if there is a container file in the path
    for ancestor in directory.ancestry():
        if ancestor.container_file:
            return HttpResponse(status=404)
    return HttpResponse(str(directory))


@collection_view
@never_cache
def rescan_directory(request, directory_pk):
    """Start a filesystem walk in the given directory."""
    dispatch_directory_walk_tasks(directory_pk)
    return HttpResponse(status=200)


@collection_view
@cache_control(**SHORT_LIVED_CACHE_OPTIONS)
def processing_status(request, hash):
    """View that checks the processing status of a given blob.

    Searches for tasks related to the given blob and filters all unfinished tasks
    (pending, started or deferred). If there are no unfinished tasks the blob has been
    processed.
    Args:
        hash: Primary key of the blob to be checked.

    Returns:
        A HTTP 200 response if the blob has been processed completely.

        A HTTP 404 response if there are unfinished tasks.
    """
    result = {'finished': False, 'done_count': 0, 'total_count': 0}
    total_tasks = models.Task.objects.filter(blob_arg__pk=hash)
    done_tasks = total_tasks.filter(Q(status='success')
                                    | Q(status='error')
                                    | Q(status='broken')
                                    )
    result['done_count'] = done_tasks.count()
    result['total_count'] = total_tasks.count()
    total_count = result['total_count']
    if total_count != 0 and result['done_count'] == total_count:
        result['finished'] = True
    return JsonResponse(result)
