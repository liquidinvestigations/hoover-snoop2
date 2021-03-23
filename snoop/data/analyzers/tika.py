"""Tasks to run Apache Tika on documents to extract text and metadata.

Module defines helper methods to work with the Tika HTTP server, as well as converting their output format
into our set of fields.

The corrected mime type is sent along with the file, since if we don't, Tika will usually fail while running
"file" and not being able to use the result.

We keep a hard-coded list of what mime types to send to Tika. We should probably send (almost) everything
and let them surprise us instead.
"""

from urllib.parse import urljoin
from django.conf import settings
import requests
from dateutil import parser
from ..tasks import snoop_task, SnoopTaskBroken, returns_json_blob
from ..utils import zulu
from snoop import tracing

TIKA_CONTENT_TYPES = [
    'text/plain',
    'text/html',
    'text/rtf',

    'application/pdf',

    'application/msword',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.template',
    'application/vnd.ms-word.document.macroEnabled.12',
    'application/vnd.ms-word.template.macroEnabled.12',
    'application/vnd.oasis.opendocument.text',
    'application/vnd.oasis.opendocument.text-template',
    'application/rtf',

    'application/vnd.ms-excel',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.template',
    'application/vnd.ms-excel.sheet.macroEnabled.12',
    'application/vnd.ms-excel.template.macroEnabled.12',
    'application/vnd.ms-excel.addin.macroEnabled.12',
    'application/vnd.ms-excel.sheet.binary.macroEnabled.12',
    'application/vnd.oasis.opendocument.spreadsheet-template',
    'application/vnd.oasis.opendocument.spreadsheet',

    'application/vnd.openxmlformats-officedocument.presentationml.presentation',
    'application/vnd.openxmlformats-officedocument.presentationml.template',
    'application/vnd.openxmlformats-officedocument.presentationml.slideshow',
    'application/vnd.ms-powerpoint',
    'application/vnd.ms-powerpoint.addin.macroEnabled.12',
    'application/vnd.ms-powerpoint.presentation.macroEnabled.12',
    'application/vnd.ms-powerpoint.template.macroEnabled.12',
    'application/vnd.ms-powerpoint.slideshow.macroEnabled.12',
    'application/vnd.oasis.opendocument.presentation',
    'application/vnd.oasis.opendocument.presentation-template',
]


def can_process(blob):
    """Checks if Tika can process this blob's mime type."""
    if blob.mime_type in TIKA_CONTENT_TYPES:
        return True

    return False


def call_tika_server(endpoint, data, content_type):
    """Executes HTTP PUT request to Tika server.

    Args:
        endpoint: the endpoint to be appended to [snoop.defaultsettings.SNOOP_TIKA_URL][].
        data: the request object to be added to the PUT request
        content_type: content type detected by our libmagic implementation. If not supplied, Tika will run
            its own `libmagic` on it, and if that fails it will stop processing the request.
    """
    session = requests.Session()
    url = urljoin(settings.SNOOP_TIKA_URL, endpoint)
    resp = session.put(url, data=data, headers={'Content-Type': content_type})

    if resp.status_code == 422:
        raise SnoopTaskBroken("tika returned http 422, corrupt?", "tika_http_422")

    if (resp.status_code != 200
            or resp.headers['Content-Type'] != 'application/json'):
        raise RuntimeError(f"Unexpected response from tika: {resp}")

    return resp


@snoop_task('tika.rmeta')
@returns_json_blob
def rmeta(blob):
    """Task to run Tika on a given Blob."""

    with blob.open() as f, tracing.span('tika.rmeta'):
        resp = call_tika_server('rmeta/text', f, blob.content_type)

    return resp.json()


def get_date_created(rmeta):
    """Extract date created from returned Tika metadata.

    The date can show up under different keys (depending on mime type and internal Tika analyzer), so we
    have to try them all and return the first hit.
    """
    FIELDS_CREATED = ['Creation-Date', 'dcterms:created', 'meta:created',
                      'created']

    for field in FIELDS_CREATED:
        value = rmeta[0].get(field)
        if value:
            return zulu(parser.parse(value))


def get_date_modified(rmeta):
    """Extract date modified from returned Tika metadata.

    The date can show up under different keys (depending on mime type and internal Tika analyzer), so we
    have to try them all and return the first hit.
    """
    FIELDS_MODIFIED = ['Last-Modified', 'Last-Saved-Date', 'dcterms:modified',
                       'meta:modified', 'created']

    for field in FIELDS_MODIFIED:
        value = rmeta[0].get(field)
        if value:
            return zulu(parser.parse(value))
