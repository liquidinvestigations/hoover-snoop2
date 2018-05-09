import json
import logging
from datetime import datetime
import shutil
import subprocess
from contextlib import contextmanager
import time
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
        log.debug('Response: %r', resp)
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


@contextmanager
def snapshot_repo(index):
    id = f'{index}-{datetime.utcnow().isoformat().lower()}'
    repo = f'{settings.SNOOP_COLLECTIONS_ELASTICSEARCH_URL}/_snapshot/{id}'
    repo_path = f'/opt/hoover/es-snapshots/{id}'

    log.info('Create snapshot repo')
    repo_resp = put_json(repo, {
        'type': 'fs',
        'settings': {
            'location': repo_path,
            'compress': True,
        },
    })
    check_response(repo_resp)

    try:
        yield (repo, repo_path)

    finally:
        log.info('Delete snapshot repo')
        delete_resp = requests.delete(repo)
        check_response(delete_resp)

        log.info('Remove repo files')
        shutil.rmtree(repo_path)


def export_index(index, stream=None):
    with snapshot_repo(index) as (repo, repo_path):
        snapshot = f'{repo}/{index}'
        log.info('Elasticsearch snapshot %r', snapshot)

        log.info('Create snapshot')
        snapshot_resp = put_json(snapshot, {
            'indices': index,
            'include_global_state': False,
        })
        check_response(snapshot_resp)

        while True:
            status_resp = requests.get(snapshot)
            check_response(status_resp)
            state = status_resp.json()['snapshots'][0]['state']
            log.debug('Snapshot state: %r', state)

            if state == 'SUCCESS':
                log.info('Snapshot created successfully')
                break

            time.sleep(1)

        log.info('Create tar archive')
        subprocess.run(
            'tar c *',
            cwd=repo_path,
            shell=True,
            check=True,
            stdout=stream,
        )
