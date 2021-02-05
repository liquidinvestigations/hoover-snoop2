"""Task to call a service that creates pdf previews for various types of documents.

The service used can be found here: [[https://github.com/thecodingmachine/gotenberg]]
"""

from .. import models
from django.conf import settings
import requests
from ..tasks import snoop_task, SnoopTaskBroken
import os
import mimetypes


PDF_PREVIEW_MIME_TYPES = {
    'text/plain',
    'text/rtf',
    'application/msword',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    'application/msexcel',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'application/mspowerpoint',
    'application/vnd.openxmlformats-officedocument.presentationml.presentation',
    'application/vnd.oasis.opendocument.presentation',
    'application/vnd.oasis.opendocument.spreadsheet',
    'application/vnd.oasis.opendocument.text',
}
"""List of mime types that the pdf generator supports.
Based on [[https://thecodingmachine.github.io/gotenberg/#office.basic]].
"""

PDF_PREVIEW_EXTENSIONS = {
    '.txt',
    '.rtf',
    '.fodt',
    '.doc',
    '.docx',
    '.odt',
    '.xls',
    '.xlsx',
    '.ods',
    '.ppt',
    '.pptx',
    '.odp',
}
"""List of file extensions that the pdf generator supports.
Based on [[https://thecodingmachine.github.io/gotenberg/#office.basic]].
"""

PDF_PREVIEW_TIMEOUT_BASE = 60
"""Minimum number of seconds to wait for this service."""

PDF_PREVIEW_TIMEOUT_MAX = 2 * 3600
"""Maximum number of seconds to wait for this service. For PDF preview we allow 2h."""

PDF_PREVIEW_MIN_SPEED_BPS = 35 * 1024  # 35 KB/s
"""Minimum reference speed for this task. Saved as 10% of the Average Success
Speed in the Admin UI. The timeout is calculated using this value, the request
file size, and the previous `TIMEOUT_BASE` constant."""


def can_create(blob):
    """Checks if the pdf generator can process this file."""
    if blob.mime_type in PDF_PREVIEW_MIME_TYPES:
        return True


def call_pdf_generator(data, filename, size):
    """Executes HTTP PUT request to the pdf generator service."""

    url = settings.SNOOP_PDF_PREVIEW_URL + 'convert/office'

    timeout = min(PDF_PREVIEW_TIMEOUT_MAX,
                  int(PDF_PREVIEW_TIMEOUT_BASE + size / PDF_PREVIEW_MIN_SPEED_BPS))

    resp = requests.post(url, files={'files': (filename, data)}, timeout=timeout)

    if resp.status_code == 504:
        raise SnoopTaskBroken('pdf generator timed out and returned http 504', 'pdf_preview_http_504')

    if (resp.status_code != 200
            or resp.headers['Content-Type'] != 'application/pdf'):
        # print(resp.content)
        raise SnoopTaskBroken(f'pdf generator returned unexpected response {resp}',
                              'pdf_preview_http_' + str(resp.status_code))

    return resp.content


@snoop_task('pdf_preview.get_pdf')
def get_pdf(blob):
    """Calls the pdf generator for a given blob.

    Adds the pdf preview to the database
    """
    # the service needs to receive a filename but the original filename might be broken
    DEFAULT_FILENAME = 'a'
    filename = models.File.objects.filter(original=blob.pk)[0].name
    _, ext = os.path.splitext(filename)
    if ext not in PDF_PREVIEW_EXTENSIONS:
        ext = mimetypes.guess_extension(blob.mime_type)
        if ext not in PDF_PREVIEW_EXTENSIONS:
            raise SnoopTaskBroken('no valid file extension found', 'invalid_file_extension')

    with blob.open() as f:
        resp = call_pdf_generator(f, DEFAULT_FILENAME + ext, blob.size)
    blob_pdf_preview = models.Blob.create_from_bytes(resp)
    # create PDF object in pdf preview model
    _, _ = models.PdfPreview.objects.update_or_create(
        blob=blob,
        defaults={'pdf_preview': blob_pdf_preview}
    )
