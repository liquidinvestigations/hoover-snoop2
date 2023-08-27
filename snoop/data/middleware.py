"""Django middlewares for removing framework functionality.

Enforces system-wide disabling of the Login System (automatically logging everyone in as an admin with no
password visiting the page) and the CSRF protection system (that requires headers from this server to reach
the UI and back.

As stated in the readme, this Django instance leaves access control and security a problem outside its
scope. The port needs to be firewalled off and made accessible only to sysadmins debugging the Tasks table.
"""
import logging

from django.contrib.auth.models import User
from django.http import HttpResponseNotModified

from hoover.search.pdf_tools import split_pdf_file, get_pdf_info, pdf_extract_text


log = logging.getLogger(__name__)


class AutoLogin():
    """Middleware that automatically logs anonymous users in as an administrator named "root".

    Since the Django Admin can't work without the concept of users, we couldn't disable the system
    - so we use this middleware to create and log in an admin user called "root", and use the admin
    normally.
    """
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        root, created = User.objects.get_or_create(username='root')
        if created or not root.is_staff:
            root.is_staff = True
            root.is_superuser = True
            root.is_admin = True
            root.is_active = True
            root.set_password('root')
            root.save()

        request.user = root
        return self.get_response(request)


class DisableCSRF():
    """Middleware that patches requests to disable CSRF checks.

    Since the Django Admin can't work without CSRF enabled, we couldn't disable the system
    - so we use this middleware to patch it out.
    """
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        setattr(request, '_dont_enforce_csrf_checks', True)
        return self.get_response(request)


class PdfToolsMiddleware:
    """Split PDF files into parts depending on GET options"""
    HEADER_RANGE = 'X-Hoover-PDF-Split-Page-Range'
    HEADER_PDF_INFO = 'X-Hoover-PDF-Info'
    HEADER_PDF_EXTRACT_TEXT = 'X-Hoover-PDF-Extract-Text'
    HEADER_PDF_EXTRACT_TEXT_METHOD = 'X-Hoover-PDF-Extract-Text-Method'
    HEADER_IGNORED = 'X-Hoover-PDF-Ignored'

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # pass over unrelated requests
        get_info = request.GET.get(self.HEADER_PDF_INFO, '')
        get_range = request.GET.get(self.HEADER_RANGE, '')
        get_text = request.GET.get(self.HEADER_PDF_EXTRACT_TEXT, '')
        get_text_method = request.GET.get(self.HEADER_PDF_EXTRACT_TEXT_METHOD, '')

        if (
            request.method != 'GET'
            or not (
                get_info or get_range or get_text
            )
        ):
            return self.get_response(request)

        # pop off the GET params read above
        request.GET = request.GET.copy()
        if get_info:
            del request.GET[self.HEADER_PDF_INFO]
        if get_range:
            del request.GET[self.HEADER_RANGE]
        if get_text:
            del request.GET[self.HEADER_PDF_EXTRACT_TEXT]
        if get_text_method:
            del request.GET[self.HEADER_PDF_EXTRACT_TEXT_METHOD]

        # pop the request If-Modified-Since and If-None-Match
        # so the upstream service doesn't return 304 -- we will
        if request.headers.get('If-Modified-Since'):
            req_cache_date = request.headers['If-Modified-Since']
            del request.headers['If-Modified-Since']
        else:
            req_cache_date = None

        if request.headers.get('If-None-Match'):
            req_cache_etag = request.headers['If-None-Match']
            del request.headers['If-None-Match']
        else:
            req_cache_etag = None

        response = self.get_response(request)
        response['Etag'] = (
            response.headers.get('Etag', '')
            + ':' + get_info
            + ':' + get_range
            + ':' + get_text
        )
        if (
            req_cache_date and req_cache_etag
            and req_cache_date == response.headers.get('Last-Modified')
            and req_cache_etag == response.headers.get('Etag')
            and 'no-cache' not in request.headers.get('Cache-Control', '')
            and 'no-cache' not in request.headers.get('Pragma', '')
        ):
            return HttpResponseNotModified()

        # mark failure in case of unsupported operation
        if (
            request.headers.get('range')
            or response.status_code != 200
            or response.headers.get('Content-Type') != 'application/pdf'
        ):
            response = self.get_response(request)
            response[self.HEADER_IGNORED] = '1'
            return response

        assert response.streaming, 'pdf split - can only be used with streaming repsonses'
        # assert not response.is_async, 'pdf split - upstream async not supported'

        log.warning('PDF tools request STARTING... ')

        # handle range query
        if get_range:
            # parse the range to make sure it's 1-100 and not some bash injection
            page_start, page_end = get_range.split('-')
            page_start, page_end = int(page_start), int(page_end)
            assert 0 < page_start <= page_end, 'bad page interval'
            _range = f'{page_start}-{page_end}'

            response[self.HEADER_RANGE] = _range
            response.streaming_content = split_pdf_file(response.streaming_content, _range)

        if get_info:
            response.streaming_content = get_pdf_info(response.streaming_content)
            response['content-type'] = 'application/json'
            response[self.HEADER_PDF_INFO] = '1'
        elif get_text:
            response.streaming_content = pdf_extract_text(response.streaming_content, get_text_method)
            response['content-type'] = 'text/plain; charset=utf-8'
            response[self.HEADER_PDF_EXTRACT_TEXT] = '1'

        del response.headers['Content-Length']
        del response.headers['Content-Disposition']
        response.headers['Accept-Ranges'] = 'bytes'
        return response
