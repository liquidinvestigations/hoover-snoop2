from .. import models
# from django.conf import settings
# from urllib.parse import urljoin
import requests
from ..tasks import snoop_task


def call_thumbnails_service(data):
    """Executes HTTP PUT request to Thumbnail service.

    Args:
        endpoint: the endpoint to be appended to [snoop.defaultsettings.SNOOP_TIKA_URL][].
        data: the file for which a thumbnail will be created.
        """

    session = requests.Session()
    url = 'http://host.docker.internal:5000/thumbnail'
    resp = session.put(url, data=data)

    if (resp.status_code != 200
            or resp.headers['Content-Type'] != 'image/json'):
        raise RuntimeError(f"Unexpected response from tika: {resp}")

    return resp.content


@snoop_task('thumbnails.get_thumbnail')
def get_thumbnail(blob):
    """Function that calls the thumbnail service for a given blob.

    Returns the primary key of the created thumbnail blob.
    """

    with blob.open() as f:
        resp = call_thumbnails_service(f)
    blob_pk = models.Blob.create_from_bytes(resp).pk
    return blob_pk
