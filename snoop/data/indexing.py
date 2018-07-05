import sys
import json
import logging
import tarfile
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
    },
    "index": {
        "max_result_window": "20000"
    }
}

CONFIG = {'mappings': MAPPINGS, 'settings': SETTINGS}


def put_json(url, data):
    return requests.put(
        url,
        data=json.dumps(data),
        headers={'Content-Type': 'application/json'},
    )


def post_json(url, data):
    return requests.post(
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


def delete_index(index):
    url = f'{settings.SNOOP_COLLECTIONS_ELASTICSEARCH_URL}/{index}'
    log.info("DELETE %s", url)
    delete_resp = requests.delete(url)
    log.debug('Response: %r', delete_resp)


def create_index(index):
    url = f'{settings.SNOOP_COLLECTIONS_ELASTICSEARCH_URL}/{index}'
    log.info("PUT %s", url)
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


def import_index(index, delete=False, stream=None):
    if delete:
        delete_index(index)

    with snapshot_repo(index) as (repo, repo_path):
        log.info('Unpack tar archive')

        tar = tarfile.open(mode='r|*', fileobj=stream or sys.stdin.buffer)
        tar.extractall(repo_path)
        tar.close()

        snapshots_resp = requests.get(f'{repo}/*')
        check_response(snapshots_resp)
        for s in snapshots_resp.json()['snapshots']:
            if s['state'] == 'SUCCESS':
                [snapshot_index] = s['indices']
                if snapshot_index != index:
                    continue
                snapshot = f'{repo}/{s["snapshot"]}'
                break
        else:
            raise RuntimeError(f"No snapshots for index {index}")

        log.info("Starting restore for index %r as %r", snapshot_index, index)
        restore = f'{snapshot}/_restore'
        restore_resp = post_json(restore, {
            'indices': index,
            'include_global_state': False,
            'include_aliases': False,
        })
        check_response(restore_resp)
        assert restore_resp.json()['accepted']

        es_url = settings.SNOOP_COLLECTIONS_ELASTICSEARCH_URL
        status = f'{es_url}/{index}/_recovery'
        while True:
            status_resp = requests.get(status)
            check_response(status_resp)
            if not status_resp.json():
                log.debug("Waiting for restore to start")
                time.sleep(1)
                continue

            for shard in status_resp.json()[index]['shards']:
                stage = shard['stage']
                if stage != 'DONE':
                    log.debug("Shard %r stage=%r", shard['id'], stage)
                    time.sleep(1)
                    break

            else:
                log.info('Snapshot restored successfully')
                break
