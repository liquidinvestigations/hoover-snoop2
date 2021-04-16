from .. import models
# from django.conf import settings
from urllib.parse import urljoin
import requests
from ..tasks import snoop_task
import os


def call_thumbnails_service(data, size):
    """Executes HTTP PUT request to Thumbnail service.

    Args:
        endpoint: the endpoint to be appended to [snoop.defaultsettings.SNOOP_TIKA_URL][].
        data: the file for which a thumbnail will be created.
        """
    SNOOP_THUMBNAIL_URL = os.environ.get('SNOOP_THUMBNAIL_URL')

    url = SNOOP_THUMBNAIL_URL + f'preview/{size}x{size}'

    session = requests.Session()
    resp = session.post(url, files={'file': data})

    if (resp.status_code != 200
            or resp.headers['Content-Type'] != 'image/jpeg'):
        raise RuntimeError(f"Unexpected response from thumbnails-service: {resp}")
    return resp.content


@snoop_task('thumbnails.get_thumbnail')
def get_thumbnail(blob, size):
    """Function that calls the thumbnail service for a given blob.

    Returns the primary key of the created thumbnail blob.
    """

    with blob.open() as f:
        resp = call_thumbnails_service(f)
        blob_thumb = models.Blob.create_from_bytes(resp, size)
    return blob_thumb
