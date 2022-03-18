"""Task to call a service that runs object detection and/or image classification on images"""

import io
import logging

import requests
from django.conf import settings
from PIL import Image, UnidentifiedImageError

from .. import models
from ..tasks import SnoopTaskBroken, returns_json_blob, snoop_task


log = logging.getLogger(__name__)

PROBABILITY_LIMIT = 20

IMAGE_CLASSIFICATION_MIME_TYPES = {
    'image/bmp',
    'image/gif',
    'image/jpeg',
    'image/x-icns',
    'image/x-icon',
    'image/jp2',
    'image/png',
    'image/x-portable-anymap',
    'image/x-portable-bitmap',
    'image/x-portable-graymap',
    'image/x-portable-pixmap',
    'image/sgi',
    'image/x-targa',
    'image/x-tga',
    'image/tiff',
    'image/webp',
    'image/x‑xbitmap',
    'image/x-xbm',
    'image/vnd.microsoft.icon',
    'image/vnd.adobe.photoshop',
    'image/x-xpixmap',
}
"""Based on https://pillow.readthedocs.io/en/stable/handbook/image-file-formats.html#image-file-formats"""

CLASSIFICATION_TIMEOUT_BASE = 60
"""Minimum number of seconds to wait for this service."""
CLASSIFICATION_TIMEOUT_MAX = 1000
"""Maximum number of seconds to wait for this service."""

CLASSIFICATION_MIN_SPEED_BPS = 50 * 1024  # 50 KB/s
"""Minimum reference speed for this task. Saved as 10% of the Average Success
Speed in the Admin UI. The timeout is calculated using this value, the request
file size, and the previous `TIMEOUT_BASE` constant."""

DETECT_OBJECTS_TIMEOUT_BASE = 120
"""Minimum number of seconds to wait for this service."""

DETECT_OBJECTS_TIMEOUT_MAX = 1000
"""Maximum number of seconds to wait for this service."""

DETECT_OBJECTS_MIN_SPEED_BPS = 8 * 1024  # 8 KB/s
"""Minimum reference speed for this task. Saved as 10% of the Average Success
Speed in the Admin UI. The timeout is calculated using this value, the request
file size, and the previous `TIMEOUT_BASE` constant."""


def can_detect(blob):
    """Return true if the image type is supported.

    This will return true for all image types that can be converted into .jpg.
    """
    if blob.mime_type in IMAGE_CLASSIFICATION_MIME_TYPES:
        return True


def convert_image(blob):
    """Convert image to jpg"""
    with blob.open() as i:
        try:
            image = Image.open(i)
        except UnidentifiedImageError:
            raise SnoopTaskBroken('Cannot convert image to jpg.',
                                  'image_classification_jpg_conversion_error')
        if image.mode != 'RGB':
            image = image.convert('RGB')
        buf = io.BytesIO()
        image.save(buf, format='JPEG')
    return buf.getvalue()


def call_object_detection_service(imagedata, filename, data_size):
    """Executes HTTP PUT request to the object detection service."""

    url = settings.SNOOP_OBJECT_DETECTION_URL
    timeout = min(
        DETECT_OBJECTS_TIMEOUT_MAX,
        int(DETECT_OBJECTS_TIMEOUT_BASE + data_size / DETECT_OBJECTS_MIN_SPEED_BPS)
    )

    resp = requests.post(url, files={'image': (filename, imagedata)}, timeout=timeout)

    if (resp.status_code != 200 or resp.headers['Content-Type'] != 'application/json'):
        log.error(resp.content)
        raise SnoopTaskBroken('Object detection service could not process the image',
                              f'object_detection_http_{resp.status_code}')

    return resp.json()


def call_image_classification_service(imagedata, filename, data_size):
    """Executes HTTP PUT request to the object detection service."""

    url = settings.SNOOP_IMAGE_CLASSIFICATION_URL
    timeout = min(
        CLASSIFICATION_TIMEOUT_MAX,
        int(CLASSIFICATION_TIMEOUT_BASE + data_size / CLASSIFICATION_MIN_SPEED_BPS)
    )

    resp = requests.post(url, files={'image': (filename, imagedata)}, timeout=timeout)

    if (resp.status_code != 200 or resp.headers['Content-Type'] != 'application/json'):
        log.error(resp.content)
        raise SnoopTaskBroken('Image classification service could not process the image',
                              f'image_classification_http_{resp.status_code}')

    return resp.json()


@snoop_task('image_classification.detect_objects')
@returns_json_blob
def detect_objects(blob):
    """Calls the object detection service for an image blob.

    Filters the results by probability. The limit is given by PROBABILITY_LIMIT.
    """

    filename = models.File.objects.filter(original=blob.pk)[0].name
    if blob.mime_type == 'image/jpeg':
        with blob.open() as f:
            detections = call_object_detection_service(f, filename, blob.size)
    else:
        image_bytes = convert_image(blob)
        image = io.BytesIO(image_bytes)
        detections = call_object_detection_service(image, filename, blob.size)

    filtered_detections = []
    for hit in detections:
        score = int(hit.get('percentage_probability'))
        if score >= PROBABILITY_LIMIT:
            filtered_detections.append({'object': hit.get('name'), 'score': score})
    return filtered_detections


@snoop_task('image_classification.classify_image')
@returns_json_blob
def classify_image(blob):
    """Calls the image classification service for an image blob.

    Filters the results by probability. The limit is given by PROBABILITY_LIMIT.
    """

    filename = models.File.objects.filter(original=blob.pk)[0].name
    if blob.mime_type == 'image/jpeg':
        with blob.open() as f:
            predictions = call_image_classification_service(f, filename, blob.size)
    else:
        image_bytes = convert_image(blob)
        image = io.BytesIO(image_bytes)
        predictions = call_image_classification_service(image, filename, blob.size)

    filtered_predictions = []
    for hit in predictions:
        score = int(hit[1])
        if score >= PROBABILITY_LIMIT:
            filtered_predictions.append({'class': hit[0], 'score': score})
    return filtered_predictions
