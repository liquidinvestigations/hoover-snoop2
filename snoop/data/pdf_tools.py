"""Helpers for working with PDF files
(splitting into pages, fetching info, extracting text for UI Find tool)"""

import math
from tempfile import NamedTemporaryFile, TemporaryDirectory
import subprocess
import logging
import os

from django.http import HttpResponse, JsonResponse, FileResponse
from django.core.cache import caches as django_caches

from .utils import flock_blocking

log = logging.getLogger(__name__)
MAX_PDF_PAGES_PER_CHUNK = 6000


def run_script(script, timeout='120s', kill='130s'):
    """Call the script and return the stdout; add 2min timeout"""
    # vandalize script so we drop very long STDERR messages from the logs
    # qpdf is sometimes very spammy with content warnings
    with TemporaryDirectory(prefix='pdf-tools-pwd-') as pwd:
        script = script + ' 2> >(head -c200 >&2)'
        script = f'cd {pwd}; ' + script
        cmd = ['/usr/bin/timeout', '-k', kill, timeout, '/bin/bash', '-exo', 'pipefail', '-c', script]
        log.warning('+ %s', script)
        return subprocess.check_output(cmd, cwd=pwd)


def get_pdf_info(path):
    """streaming wrapper to extract pdf info json (page count, chunks)"""
#    script = "export JAVA_TOOL_OPTIONS='-Xmx3g'; pdftk - dump_data | grep NumberOfPages | head -n1"
    # script = "pdfinfo -  | grep Pages | head -n1"
    script = f"qpdf --show-npages {path}"
    page_count = int(run_script(script).decode('ascii'))
    size_mb = round(os.stat(path).st_size / 2**20, 3)
    DESIRED_CHUNK_MB = 25
    chunk_count = max(1, int(math.ceil(size_mb / DESIRED_CHUNK_MB)))
    pages_per_chunk = int(math.ceil((page_count + 1) / chunk_count))
    pages_per_chunk = min(pages_per_chunk, MAX_PDF_PAGES_PER_CHUNK)
    expected_chunk_size_mb = round(size_mb / chunk_count, 3)
    chunks = []
    for i in range(0, chunk_count):
        a = 1 + i * pages_per_chunk
        b = a + pages_per_chunk - 1
        b = min(b, page_count)
        chunks.append(f'{a}-{b}')

    return {
        'size_mb': size_mb,
        'expected_chunk_size_mb': expected_chunk_size_mb,
        'page_count': page_count,
        'chunks': chunks,
    }


def split_pdf_file(path, _range, dest_path):
    """streaming wrapper to split pdf file into a page range."""
    script = (
        " qpdf --empty --no-warn --warning-exit-0 --deterministic-id "
        " --object-streams=generate  --remove-unreferenced-resources=yes "
        " --no-original-object-ids "
        f" --pages {path} {_range}  -- {dest_path}"
    )
    run_script(script)


def pdf_extract_text(infile, outfile):
    """Extract pdf text using javascript."""
    script = f'/opt/hoover/snoop/pdf-tools/run.sh {infile} {outfile}'
    run_script(script)


def apply_pdf_tools(request, blob, max_size_before_stream):
    """
    Apply processing to PDF files based on GET params.

    Request GET params:
        - 'X-Hoover-PDF-Info'
            - if set, return page count, and a division of pages
        - 'X-Hoover-PDF-Split-Page-Range'
        - 'X-Hoover-PDF-Extract-Text'
    """

    HEADER_RANGE = 'X-Hoover-PDF-Split-Page-Range'
    HEADER_PDF_INFO = 'X-Hoover-PDF-Info'
    HEADER_PDF_EXTRACT_TEXT = 'X-Hoover-PDF-Extract-Text'

    # pass over unrelated requests
    _get_info = request.GET.get(HEADER_PDF_INFO, '')
    _get_range = request.GET.get(HEADER_RANGE, '')
    _get_text = request.GET.get(HEADER_PDF_EXTRACT_TEXT, '')

    if (
        request.method != 'GET'
        or not (
            _get_info or _get_range or _get_text
        )
    ):
        return None

    if request.headers.get('Range'):
        log.warning('PDF Tools: Reject Range query')
        return HttpResponse('X-Hoover-PDF does not work with HTTP-Range', status=400)

    if (
        (_get_info and _get_range)
        or (_get_info and _get_text)
    ):
        log.warning('PDF Tools: Reject Bad Arguments')
        return HttpResponse('X-Hoover-PDF-Get-Info must be only arg', status=400)

    def _add_headers(response, content_type):
        response['Content-Type'] = content_type
        response[HEADER_PDF_INFO] = _get_info
        response[HEADER_RANGE] = _get_range
        response[HEADER_PDF_EXTRACT_TEXT] = _get_text
        return response
    with blob.mount_path() as blob_path, \
            NamedTemporaryFile(prefix='pdf-split') as split_file, \
            NamedTemporaryFile(prefix='pdf-text') as text_file:
        if _get_info:
            return JsonResponse(get_pdf_info(blob_path))

        # for very big PDFs >50MB, use lockfile so we don't OOM...
        blob_size_mb = blob.size / 2**20
        if blob_size_mb > 50:
            _func = _lock_get_range_or_text
        else:
            _func = _do_get_range_or_text

        return _func(
            blob.pk,
            blob_path,
            _get_range,
            split_file,
            _get_text,
            text_file,
            _add_headers,
            max_size_before_stream,
        )


@flock_blocking  # qpdf takes lots of RAM, make sure only run 1/container
def _lock_get_range_or_text(*args, **kw):
    """Lockfile-protected execution of _do_get_range_or_text.
    Useful to limit parallel execution of very big pdf"""
    return _do_get_range_or_text(*args, **kw)


def _do_get_range_or_text(
    blob_pk,
    blob_path,
    _get_range,
    split_file,
    _get_text,
    text_file,
    _add_headers,
    max_size_before_stream,
):
    """Run the different PDF parse flows, caching intermediary results"""
    cache_pdf_pages = django_caches['pdf_pages']
    out_file = None
    content_type = None
    if _get_range:
        # parse the range to make sure it's 1-100 and not some bash injection
        page_start, page_end = _get_range.split('-')
        page_start, page_end = int(page_start), int(page_end)
        assert 0 < page_start <= page_end, 'bad page interval'
        assert 0 <= page_end - page_start <= MAX_PDF_PAGES_PER_CHUNK + 10, \
            'too many pages'
        _range = f'{page_start}-{page_end}'

        # we need to cache the split pages for a while, since the UI will hit both
        # endpoints with and without extract-text, in short succession.
        # Cache only needs to live between the two fetches,
        # since we will cache the end result too for a long time.
        CACHE_TIMEOUT = 24 * 3600
        _cache_key = 'pdf-pages-' + blob_pk + ':' + _range
        out_file = split_file.name
        content_type = 'application/pdf'
        # pull from cache, if exists
        if cached_content := cache_pdf_pages.get(_cache_key):
            split_file.write(cached_content)
            log.warning('PDF PAGE CACHE HIT: %s', _cache_key)
        else:
            # do the split
            log.warning('PDF PAGE CACHE MISS: %s', _cache_key)
            split_pdf_file(blob_path, _range, split_file.name)
            _size = os.stat(split_file.name).st_size
            if _size < max_size_before_stream:
                # write to cache
                cache_pdf_pages.add(
                    _cache_key,
                    split_file.read(),
                    timeout=CACHE_TIMEOUT,
                )
                log.warning('PDF PAGE CACHE ADD: %s', _cache_key)
            else:
                log.warning('PDF PAGE CACHE REJECT: %s', _cache_key)
    if _get_text:
        out_file = text_file.name
        content_type = 'application/json'
        if _get_range:
            pdf_extract_text(split_file.name, text_file.name)
        else:
            pdf_extract_text(blob_path, text_file.name)

    assert out_file is not None
    assert content_type is not None

    out_file_size = os.stat(out_file).st_size
    with open(out_file, 'rb') as f:
        if out_file_size < max_size_before_stream:
            response = HttpResponse(f.read())
        else:
            response = FileResponse(f)
        return _add_headers(response, content_type)
