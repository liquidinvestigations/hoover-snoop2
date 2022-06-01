"""Task to call a service that creates pdf previews for various types of documents.

The service used can be found here: [[https://github.com/thecodingmachine/gotenberg]]
"""
import os
import logging

from django.conf import settings
import requests
import mimetypes

from .. import models
from ..tasks import snoop_task, SnoopTaskBroken
from ..collections import current as current_collection

log = logging.getLogger(__name__)


PDF_PREVIEW_MIME_TYPES = {
    'application/x-bibtex',
    'application/msword',
    # 'application/xml',  NO TEXT!
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    'application/vnd.oasis.opendocument.text',
    # 'text/html',  # no html
    'application/x-latex',
    # 'text/plain',  NO TEXT!
    'application/vnd.oasis.opendocument.text',
    'application/vnd.oasis.opendocument.text-template',
    'chemical/x-pdb',
    # 'application/pdf',  # no PDF preview for PDFs
    'application/x-pocket-word',
    'application/rtf',
    'application/vnd.stardivision.writer',
    'application/vnd.sun.xml.writer.template',
    'application/vnd.sun.xml.writer',
    'application/vnd.ms-works',
    'application/epub+zip',
    # 'image/png',  NO IMAGES!
    # 'image/bmp',
    # 'image/emf',
    'application/postscript',
    'application/vnd.oasis.opendocument.graphics',
    # 'image/gif',  NO IMAGES!
    # 'image/jpeg', NO IMAGES!
    'application/vnd.oasis.opendocument.graphics-template',
    # 'image/x-portable-bitmap', NO IMAGES!
    # 'image/x-pict', NO IMAGES!
    # 'image/x-portable-graymap', NO IMAGES!
    # 'image/x-portable-pixmap', NO IMAGES!
    # 'image/cmu-raster', NO IMAGES!
    'application/vnd.sun.xml.draw.template',
    # 'image/svg+xml', NO IMAGES!
    # 'image/x-svm', NO IMAGES!
    'application/x-shockwave-flash',
    'application/vnd.sun.xml.draw',
    # 'image/tiff', NO IMAGES!
    'application/xhtml+xml',
    # 'image/x-xpixmap', NO IMAGES!
    'application/vnd.oasis.opendocument.presentation',
    'application/vnd.ms-powerpoint.presentation.macroEnabled.12',
    'application/mspowerpoint',
    'application/vnd.openxmlformats-officedocument.presentationml.presentation',
    'application/vnd.stardivision.draw',
    'application/vnd.stardivision.impress',
    'application/vnd.sun.xml.impress.template',
    'application/vnd.sun.xml.impress',
    # 'image/wmf', NO IMAGES!
    # 'text/csv',  NO TEXT!
    # 'text/csv',  NO TEXT!
    'video/x-dv',
    'application/x-dbase',
    'application/vnd.oasis.opendocument.spreadsheet',
    'application/vnd.oasis.opendocument.spreadsheet-template',
    'application/x-pocket-excel',
    'application/vnd.stardivision.calc',
    'application/vnd.ms-excel',
    'application/vnd.sun.xml.calc.template',
    'application/vnd.sun.xml.calc',
    'application/vnd.ms-excel',
    'application/excel',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
}
"""List of mime types that the pdf generator supports.
Based on [[https://gotenberg.dev/docs/modules/libreoffice]].
"""

PDF_PREVIEW_EXTENSIONS = {
    '.bib',
    '.doc',
    # '.xml',  # NO TEXT!
    '.docx',
    '.fodt',
    # '.html', no html
    '.ltx',
    # '.txt',  NO TEXT!
    '.odt',
    '.ott',
    '.pdb',
    # '.pdf',  # NO PDF PREVIEW FOR PDFs
    '.psw',
    '.rtf',
    '.sdw',
    '.stw',
    '.sxw',
    '.uot',
    '.vor',
    '.wps',
    '.epub',
    # '.png',  NO IMAGES!
    # '.bmp',  NO IMAGES!
    '.emf',
    '.eps',
    '.fodg',
    '.gif',
    '.jpg',
    '.met',
    '.odd',
    '.otg',
    '.pbm',
    '.pct',
    '.pgm',
    '.ppm',
    '.ras',
    '.std',
    '.svg',
    '.svm',
    '.swf',
    '.sxd',
    '.sxw',
    # '.tiff',  NO IMAGES!
    # '.xhtml', no html
    '.xpm',
    '.fodp',
    '.potm',
    '.pot',
    '.pptx',
    '.pps',
    '.ppt',
    '.pwp',
    '.sda',
    '.sdd',
    '.sti',
    '.sxi',
    '.uop',
    '.wmf',
    '.csv',
    '.dbf',
    '.dif',
    '.fods',
    '.ods',
    '.ots',
    '.pxl',
    '.sdc',
    '.slk',
    '.stc',
    '.sxc',
    '.uos',
    '.xls',
    '.xlt',
    '.xlsx',
    '.tif',
    # '.jpeg',  NO IMAGES!
    '.odp',
}
"""List of file extensions that the pdf generator supports.
Based on [[https://gotenberg.dev/docs/modules/libreoffice]].
"""

PDF_PREVIEW_TIMEOUT_BASE = 120
"""Minimum number of seconds to wait for this service."""

PDF_PREVIEW_TIMEOUT_MAX = 2 * 3600
"""Maximum number of seconds to wait for this service. For PDF preview we allow 2h."""

PDF_PREVIEW_MIN_SPEED_BPS = 15 * 1024  # 15 KB/s
"""Minimum reference speed for this task. Saved as 10% of the Average Success
Speed in the Admin UI. The timeout is calculated using this value, the request
file size, and the previous `TIMEOUT_BASE` constant."""


def can_create(blob):
    """Checks if the pdf generator can process this file."""
    if blob.mime_type in PDF_PREVIEW_MIME_TYPES:
        return True


def call_pdf_generator(data, filename, size):
    """Executes HTTP PUT request to the pdf generator service."""

    url = settings.SNOOP_PDF_PREVIEW_URL + 'forms/libreoffice/convert'

    timeout = min(PDF_PREVIEW_TIMEOUT_MAX,
                  int(PDF_PREVIEW_TIMEOUT_BASE + size / PDF_PREVIEW_MIN_SPEED_BPS))

    resp = requests.post(url, files={'files': (filename, data)}, timeout=timeout)

    if resp.status_code == 504:
        raise SnoopTaskBroken('pdf generator timed out and returned http 504', 'pdf_preview_http_504')

    if (resp.status_code != 200
            or resp.headers['Content-Type'] != 'application/pdf'):
        raise SnoopTaskBroken(f'pdf generator returned unexpected response {resp}',
                              'pdf_preview_http_' + str(resp.status_code))

    return resp.content


@snoop_task('pdf_preview.get_pdf', version=3, queue='pdf-preview')
def get_pdf(blob):
    """Calls the pdf generator for a given blob.

    Adds the pdf preview to the database
    """
    if not current_collection().pdf_preview_enabled \
            or not can_create(blob):
        raise SnoopTaskBroken('pdf preview disabled', 'pdf_preview_disabled')

    # the service needs to receive a filename but the original filename might be broken
    DEFAULT_FILENAME = 'a'
    try:
        filename = models.File.objects.filter(original=blob.pk)[0].name
        _, ext = os.path.splitext(filename)
    except IndexError:
        log.warning('no File entry for this object!')
        ext = None
    if ext not in PDF_PREVIEW_EXTENSIONS:
        ext = mimetypes.guess_extension(blob.mime_type)
        if ext not in PDF_PREVIEW_EXTENSIONS:
            raise SnoopTaskBroken('no valid file extension guessed', 'invalid_file_extension')

    with blob.open() as f:
        resp = call_pdf_generator(f, DEFAULT_FILENAME + ext, blob.size)
    blob_pdf_preview = models.Blob.create_from_bytes(resp)
    # create PDF object in pdf preview model
    _, _ = models.PdfPreview.objects.update_or_create(
        blob=blob,
        defaults={'pdf_preview': blob_pdf_preview}
    )
    return blob_pdf_preview
