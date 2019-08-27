import json
import logging

from django.conf import settings

from snoop.data.indexing import language_detector
from snoop.data.tasks import shaorma, returns_json_blob, ShaormaBroken
from snoop.trace import tracer

log = logging.getLogger(__name__)


def can_detect(blob):
    has_text = blob.content_type.startswith('text') or blob.content_type.startswith('application')
    return settings.DETECT_LANGUAGE and has_text


@shaorma('language.detect')
@returns_json_blob
def detect_language(blob, **depends_on):
    lang = {'lang': None}

    if not settings.DETECT_LANGUAGE:
        return lang

    tracer.current_span().add_annotation('detect language')

    tika_rmeta_blob = depends_on.get('tika_rmeta')
    if tika_rmeta_blob:
        if isinstance(tika_rmeta_blob, ShaormaBroken):
            log.debug("tika_rmeta task is broken; skipping language detection")

        else:
            with tika_rmeta_blob.open(encoding='utf8') as f:
                tika_rmeta = json.load(f)
            text = tika_rmeta[0].get('X-TIKA:content', "")
            if text:
                try:
                    lang['lang'] = language_detector(text[:2500])
                except Exception as e:
                    log.debug(f'Unable to detect language for document {id}: {e}')

    return lang
