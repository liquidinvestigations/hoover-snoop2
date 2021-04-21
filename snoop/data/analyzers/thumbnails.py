"""Task that is calling a thumbnail generation service.

Three Thumnbails in different sizes are created. The service used can be found here:
[[https://github.com/FPurchess/preview-service]].
"""

from .. import models
from django.conf import settings
import requests
from ..tasks import snoop_task, returns_json_blob, SnoopTaskBroken


def call_thumbnails_service(data, size):
    """Executes HTTP PUT request to Thumbnail service.

    Args:
        data: the file for which a thumbnail will be created.
        size: the size for the created thumbnail (thumbnail will be size x size)
        """

    url = settings.SNOOP_THUMBNAIL_URL + f'preview/{size}x{size}'
    print(url)
    print(type(data))

    session = requests.Session()
    resp = session.post(url, files={'file': data})

    if resp.status_code == 500:
        raise SnoopTaskBroken('thumbnail service returned http 500', 'thumbnail_http_500')

    if (resp.status_code != 200
            or resp.headers['Content-Type'] != 'image/jpeg'):
        raise RuntimeError(f"Unexpected response from thumbnails-service: {resp}")
    return resp.content


@snoop_task('thumbnails.get_thumbnail')
@returns_json_blob
def get_thumbnail(blob):
    """Function that calls the thumbnail service for a given blob.

    Returns the primary key of the created thumbnail blob.
    """
    SIZES = [100, 200, 400]

    for size in SIZES:
        with blob.open() as f:
            resp = call_thumbnails_service(f, size)
        blob_thumb = models.Blob.create_from_bytes(resp)
        _, _ = models.Thumbnail.objects.get_or_create(
            blob=blob,
            thumbnail=blob_thumb,
            size=size,
        )
