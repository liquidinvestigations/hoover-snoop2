from django.http import HttpResponse, JsonResponse, FileResponse, Http404
from django.shortcuts import get_object_or_404
from django.conf import settings
from . import models
from . import digests
from . import ocr
from . import collections
from .analyzers import html

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


@collection_view
def collection(request):
    col = collections.current()
    return JsonResponse({
        'name': col.name,
        'title': col.name,
        'description': col.name,
        'feed': 'feed',
        'data_urls': '{id}/json',
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
def directory(request, pk):
    directory = get_object_or_404(models.Directory.objects, pk=pk)
    return JsonResponse(digests.get_directory_data(directory))


def trim_text(data):
    """ Trim the text fields to TEXT_LIMIT chars """

    text = data['content']['text']

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
    return JsonResponse(trim_text(digests.get_document_data(digest)))


@collection_view
def document_download(request, hash, filename):
    digest = get_object_or_404(
        models.Digest.objects.only('blob'),
        blob__pk=hash,
    )
    blob = digest.blob

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
        task = get_object_or_404(models.Task.objects, func='digests.gather', args=[hash])
        blob = task.result
    return FileResponse(blob.open(), content_type=blob.content_type)


@collection_view
def document_locations(request, hash):
    digest = get_object_or_404(models.Digest.objects, blob__pk=hash)
    locations = digests.get_document_locations(digest)
    return JsonResponse({'locations': locations})
