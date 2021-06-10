"""Task to call a service that runs object detection on images"""

import requests
from .. import models
from django.conf import settings
from ..tasks import SnoopTaskBroken, snoop_task, returns_json_blob
import json


PROBABILITY_LIMIT = 20


def can_detect(blob):
    if blob.mime_type == 'image/jpeg':
        return True


def convert_image(image):
    """Convert image to jpg"""


def call_classification_service(imagedata, filename):
    """Executes HTTP PUT request to the image classification service."""

    url = settings.SNOOP_IMAGE_CLASSIFICATION_URL + '/detect-objects'

    resp = requests.post(url, files={'image': (filename, imagedata)})

    if resp.status_code == 500:
        raise SnoopTaskBroken('Image classifiaction service could not process the image',
                              'image_classification_http_500')

    if (resp.status_code != 200 or resp.headers['Content-Type'] != 'application/json'):
        print(resp.content)
        raise RuntimeError(f'Unexpected response from image classification service: {resp}')

    return resp.content


@snoop_task('image_classification.detect_objects')
@returns_json_blob
def detect_objects(blob):
    """Calls the image classification service for an image blob.

    Filters the results by probability. The limit is given by PROBABILITY_LIMIT.
    """

    filename = models.File.objects.filter(original=blob.pk)[0].name

    with blob.open() as f:
        resp_json = call_classification_service(f, filename)

    detections = json.loads(resp_json)
    filtered_detections = []
    for hit in detections:
        score = int(hit.get('percentage_probability'))
        if score >= PROBABILITY_LIMIT:
            filtered_detections.append({'object': hit.get('name'), 'score': score})
    return filtered_detections
