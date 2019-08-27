import logging

from django.conf import settings

from snoop.data import models, entity_detection
from snoop.data.analyzers.language import can_detect
from snoop.data.indexing import check_response, put_json, DOCUMENT_TYPE
from snoop.data.tasks import shaorma
from snoop.trace import tracer

log = logging.getLogger(__name__)

ES_INDEX = settings.SNOOP_COLLECTIONS_ELASTICSEARCH_INDEX
ES_URL = settings.SNOOP_COLLECTIONS_ELASTICSEARCH_URL
entities_detector = entity_detection.detectors[settings.ENTITY_DETECTOR_NAME]


def detect_text_entities(blob_id, data):
    if settings.DETECT_ENTITIES and data.get('text'):
        tracer.current_span().add_annotation('detect entities')

        try:
            data['entities'] = entities_detector(data['text'], data.get('lang'))
        except Exception as e:
            log.debug(f'Unable to detect entities for document {blob_id}: {e}')
            data['entities'] = None

        resp = put_json(f'{ES_URL}/{ES_INDEX}/{DOCUMENT_TYPE}/{blob_id}', data)
        check_response(resp)


@shaorma('entities.detect')
def detect_entities(blob, **depends_on):
    from snoop.data.digests import _get_document_content

    digest = models.Digest.objects.get(blob=blob)
    content = _get_document_content(digest)

    try:
        detect_text_entities(digest.blob.pk, content)
    except RuntimeError:
        log.exception(repr(content))
        raise


def dispatch_entity_detection():
    for file in models.File.objects.all():
        if can_detect(file.blob):
            detect_entities.laterz(file.blob, retry=True)
