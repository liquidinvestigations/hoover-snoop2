from urllib.parse import urljoin
import json
from django.conf import settings
import requests
from ..tasks import shaorma
from .. import models

TIKA_CONTENT_TYPES = [
    'text/plain',
    'text/html',

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


def call_tika_server(endpoint, data):
    url = urljoin(settings.SNOOP_TIKA_URL, endpoint)
    resp = requests.put(url, data=data)

    if (resp.status_code != 200 or
        resp.headers['Content-Type'] != 'application/json'):
        raise RuntimeError(f"Unexpected response from tika: {resp}")

    return resp


@shaorma('tika.rmeta')
def rmeta(blob):
    with blob.open() as f:
        resp = call_tika_server('/rmeta/text', f)

    data = json.dumps(resp.json(), indent=2).encode('utf8')

    with models.Blob.create() as output:
        output.write(data)

    return output.blob
