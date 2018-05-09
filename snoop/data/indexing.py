import json
import logging
import requests
from django.conf import settings

log = logging.getLogger(__name__)
DOCUMENT_TYPE = 'doc'

MAPPINGS = {
    "doc": {
        "properties": {
            "id": {"type": "string", "index": "not_analyzed"},
            "path": {"type": "string", "index": "not_analyzed"},
            "suffix": {"type": "string", "index": "not_analyzed"},
            "md5": {"type": "string", "index": "not_analyzed"},
            "sha1": {"type": "string", "index": "not_analyzed"},
            "filetype": {"type": "string", "index": "not_analyzed"},
            "lang": {"type": "string", "index": "not_analyzed"},
            "date": {"type": "date", "index": "not_analyzed"},
            "date-created": {"type": "date", "index": "not_analyzed"},
            "attachments": {"type": "boolean"},
            "message-id": {"type": "string", "index": "not_analyzed"},
            "in-reply-to": {"type": "string", "index": "not_analyzed"},
            "thread-index": {"type": "string", "index": "not_analyzed"},
            "references": {"type": "string", "index": "not_analyzed"},
            "message": {"type": "string", "index": "not_analyzed"},
            "word-count": {"type": "integer"},
            "rev": {"type": "integer"},
            "content-type": {"type": "string", "index": "not_analyzed"},
            "size": {"type": "integer"},
        }
    }
}

SETTINGS = {
    "analysis": {
        "analyzer": {
            "default": {
                "tokenizer": "standard",
                "filter": ["standard", "lowercase", "asciifolding"],
            }
        }
    }
}

CONFIG = {'mappings': MAPPINGS, 'settings': SETTINGS}


def put_json(url, data):
    return requests.put(
        url,
        data=json.dumps(data),
        headers={'Content-Type': 'application/json'},
    )


def check_response(resp):
    if 200 <= resp.status_code < 300:
        log.info('Response: %r', resp)
    else:
        log.error('Response: %r', resp)
        log.error('Response text:\n%s', resp.text)
        raise RuntimeError('Put request failed: %r' % resp)


def index(index, id, data):
    index_url = f'{settings.SNOOP_COLLECTIONS_ELASTICSEARCH_URL}/{index}'
    resp = put_json(f'{index_url}/{DOCUMENT_TYPE}/{id}', data)
    check_response(resp)


def resetindex(index, clobber=True):
    url = f'{settings.SNOOP_COLLECTIONS_ELASTICSEARCH_URL}/{index}'

    if clobber:
        log.info('%s Elasticsearch DELETE', DOCUMENT_TYPE)
        delete_resp = requests.delete(url)
        check_response(delete_resp)

    log.info('%s Elasticsearch PUT', DOCUMENT_TYPE)
    put_resp = put_json(url, CONFIG)
    check_response(put_resp)
