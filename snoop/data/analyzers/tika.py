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
from ._tika_mime_types import TIKA_MIME_TYPES

TIKA_EXPECT_FAIL_ABOVE_FILE_SIZE = 50 * 2 ** 20
"""Turn unexpected failures into permanent ones for arguments above this size.

Tika may run out of memory or otherwise fail on very large files, causing the wrong type of error.
"""


TIKA_MIME_TYPES_ORIG = {
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

    'application/csv',
    "application/tab-separated-values",
}
ALL_TIKA_MIME_TYPES = TIKA_MIME_TYPES | TIKA_MIME_TYPES_ORIG

TIKA_TIMEOUT_BASE = 120
"""Minimum number of seconds to wait for this service."""

TIKA_TIMEOUT_MAX = 24 * 3600
"""Maximum number of seconds to wait for this service. For tika we set 24h."""

TIKA_MIN_SPEED_BPS = 100 * 1024  # 100 KB/s
"""Minimum reference speed for this task. Saved as 10% of the Average Success
Speed in the Admin UI. The timeout is calculated using this value, the request
file size, and the previous `TIMEOUT_BASE` constant."""


def can_process(blob):
    """Checks if Tika can process this blob's mime type."""

    if blob.mime_type in ALL_TIKA_MIME_TYPES:
        return True

    return False


def call_tika_server(endpoint, data, content_type, data_size):
    """Executes HTTP PUT request to Tika server.

    Args:
        endpoint: the endpoint to be appended to [snoop.defaultsettings.SNOOP_TIKA_URL][].
        data: the request object to be added to the PUT request
        content_type: content type detected by our libmagic implementation. If not supplied, Tika will run
            its own `libmagic` on it, and if that fails it will stop processing the request.
    """

    timeout = min(TIKA_TIMEOUT_MAX,
                  int(TIKA_TIMEOUT_BASE + data_size / TIKA_MIN_SPEED_BPS))

    session = requests.Session()
    url = urljoin(settings.SNOOP_TIKA_URL, endpoint)
    resp = session.put(url, data=data, headers={'Content-Type': content_type}, timeout=timeout)

    if resp.status_code == 422:
        raise SnoopTaskBroken("tika returned http 422, corrupt", "tika_http_422")

    if resp.status_code == 415:
        raise SnoopTaskBroken("tika returned http 415, unsupported media type", "tika_http_415")

    # When running OOM, Tika returns 404 (from load balancer after crash), 500, 502 and any other
    # combination of status codes. We mark this as Broken instead of a normal failure to continue normal
    # processing in case of Tika OOM.
    if 400 <= resp.status_code < 600 and data_size > TIKA_EXPECT_FAIL_ABOVE_FILE_SIZE:
        raise SnoopTaskBroken(
            f"tika returned http {resp.status_code} while running on large file",
            "tika_error_on_large_file",
        )

    if (resp.status_code != 200
            or resp.headers['Content-Type'] != 'application/json'):
        raise SnoopTaskBroken(
            f"tika returned unexpected response http {resp.status_code}",
            "tika_http_" + str(resp.status_code),
        )

    return resp


@snoop_task('tika.rmeta')
@returns_json_blob
def rmeta(blob):
    """Task to run Tika on a given Blob."""

    with blob.open() as f, tracing.span('tika.rmeta'):
        resp = call_tika_server('rmeta/text', f, blob.content_type, blob.size)

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


def convert_for_indexing(rmeta_obj):
    """Convert the dict returned by Tika's `rmeta` endpoint into a list of `K: V` strings that we can
    directly index into Elasticsearch. Also returns a list of keys present, to be indexed as keywords.

    Tika returns over 500 different fields for our test data, and the ES maximum field count is 1000.
    So we folde them all into one single field.

    Because Elasticsearch 6 requires all values in a field to be of a same type, we must convert all the
    dict values to a single type (in our case, string). We replace the main `text`fields if they exist with
    `None (keys called `X-TIKA:content`). We also truncate all values to 4K chars to avoid any other
    duplication with main text fields and keep this of a lower size.

    """

    REMOVE_KEYS = {'X-TIKA:content', 'Message:Raw-Header'}
    TRUNCATE_LIMIT = 2 ** 12

    def iterate_obj(obj, path=""):
        if isinstance(obj, list):
            for x in obj:
                yield from iterate_obj(x, path)
        elif isinstance(obj, dict):
            for x in obj:
                yield from iterate_obj(obj[x], path + '.' + x)
        else:
            # skip first . in path
            path = path[1:]
            # remove keys for text and email headers (as they're handled separately)
            if not any(path.startswith(x) for x in REMOVE_KEYS):
                yield path, str(obj)[:TRUNCATE_LIMIT].strip()

    # skip first item
    rmeta_obj = rmeta_obj[0]
    return {'tika': [path + ': ' + value for path, value in iterate_obj(rmeta_obj)],
            'tika-key': list(set(path for path, _ in iterate_obj(rmeta_obj)))}
