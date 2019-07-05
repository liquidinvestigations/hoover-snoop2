from urllib.parse import urljoin
from django.conf import settings
import requests
from dateutil import parser
from ..tasks import shaorma, ShaormaBroken, returns_json_blob
from ..utils import zulu
from snoop.trace import tracer

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
    if blob.mime_type in TIKA_CONTENT_TYPES:
        return True

    return False


session = requests.Session()


def call_tika_server(endpoint, data):
    url = urljoin(settings.TIKA_URL, endpoint)
    resp = session.put(url, data=data)

    if resp.status_code == 422:
        raise ShaormaBroken("tika returned http 422, corrupt?", "tika_http_422")

    if (resp.status_code != 200
            or resp.headers['Content-Type'] != 'application/json'):
        raise RuntimeError(f"Unexpected response from tika: {resp}")

    return resp


@shaorma('tika.rmeta')
@returns_json_blob
def rmeta(blob):
    with blob.open() as f, tracer.span('tika.rmeta'):
        resp = call_tika_server('/rmeta/text', f)

    return resp.json()


def get_date_created(rmeta):
    FIELDS_CREATED = ['Creation-Date', 'dcterms:created', 'meta:created',
                      'created']

    for field in FIELDS_CREATED:
        value = rmeta[0].get(field)
        if value:
            return zulu(parser.parse(value))


def get_date_modified(rmeta):
    FIELDS_MODIFIED = ['Last-Modified', 'Last-Saved-Date', 'dcterms:modified',
                       'meta:modified', 'created']

    for field in FIELDS_MODIFIED:
        value = rmeta[0].get(field)
        if value:
            return zulu(parser.parse(value))
