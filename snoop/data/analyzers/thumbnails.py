from .. import models
from django.conf import settings
from urllib.parse import urljoin
import requests


def call_thumbnails_service(endpoint, data):
    """Executes HTTP PUT request to Thumbnail service.

    Args:
        endpoint: the endpoint to be appended to [snoop.defaultsettings.SNOOP_TIKA_URL][].
        data: the file for which a thumbnail will be created.
        """

    session = requests.Session()
    url = 'http://127.0.0.1:5000/thumbnail'
    resp = session.put(url, data=data)

    if (resp.status_code != 200
            or resp.headers['Content-Type'] != 'application/json'):
        raise RuntimeError(f"Unexpected response from tika: {resp}")

    return resp
