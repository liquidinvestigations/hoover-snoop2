from contextlib import contextmanager
from datetime import datetime
import json
import logging
import shutil
import subprocess
import sys
import tarfile
import time

from django.conf import settings
import requests
from snoop.data import collections

log = logging.getLogger(__name__)
DOCUMENT_TYPE = 'doc'
ES_URL = settings.SNOOP_COLLECTIONS_ELASTICSEARCH_URL

PUBLIC_TAGS_FIELD_NAME = 'tags'
PRIVATE_TAGS_FIELD_NAME_PREFIX = 'priv-tags.'

MAPPINGS = {
    "doc": {
        "properties": {
            "attachments": {"type": "boolean"},
            "content-type": {"type": "keyword"},
            "date": {"type": "date"},
            "date-created": {"type": "date"},
            "email-domains": {"type": "keyword"},
            "filetype": {"type": "keyword"},
            "id": {"type": "keyword"},
            "in-reply-to": {"type": "keyword"},
            "lang": {"type": "keyword"},
            "md5": {"type": "keyword"},
            "message": {"type": "keyword"},
            "message-id": {"type": "keyword"},
            "path": {"type": "keyword"},
            "path-text": {"type": "text"},
            "path-parts": {"type": "keyword"},
            "references": {"type": "keyword"},
            "rev": {"type": "integer"},
            "sha1": {"type": "keyword"},
            "size": {"type": "integer"},
            "suffix": {"type": "keyword"},
            "thread-index": {"type": "keyword"},
            "word-count": {"type": "integer"},
            "ocr": {"type": "boolean"},
            "ocrpdf": {"type": "boolean"},
            "ocrimage": {"type": "boolean"},
            PUBLIC_TAGS_FIELD_NAME: {"type": "keyword"},
            # remove the trailing '.' here
            PRIVATE_TAGS_FIELD_NAME_PREFIX[:-1]: {"type": "object"},
        },
        "dynamic_templates": [
            {
                "private_tags_are_keywords": {
                    "match_mapping_type": "*",
                    "path_match": PRIVATE_TAGS_FIELD_NAME_PREFIX + "*",
                    "mapping": {"type": "keyword"},
                },
            },
        ],
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
        log.error('Response text: %s', resp.text)
        raise RuntimeError('Put request failed: %r' % resp)


def index(id, data):
    es_index = collections.current().es_index

    index_url = f'{ES_URL}/{es_index}'
    resp = put_json(f'{index_url}/{DOCUMENT_TYPE}/{id}', data)

    check_response(resp)


def delete_doc(id):
    es_index = collections.current().es_index
    index_url = f'{ES_URL}/{es_index}'
    resp = requests.delete(f'{index_url}/{DOCUMENT_TYPE}/{id}')
    check_response(resp)


def delete_index_by_name(name):
    url = f'{ES_URL}/{name}'
    log.info("DELETE %s", url)
    delete_resp = requests.delete(url)
    log.debug('Response: %r', delete_resp)


def delete_index():
    es_index = collections.current().es_index
    delete_index_by_name(es_index)


def index_exists():
    es_index = collections.current().es_index
    head_resp = requests.head(f"{ES_URL}/{es_index}")
    return head_resp.status_code == 200


def create_index():
    es_index = collections.current().es_index
    url = f'{ES_URL}/{es_index}'
    log.info("PUT %s", url)
    put_resp = put_json(url, CONFIG)
    check_response(put_resp)


def update_mapping():
    es_index = collections.current().es_index
    url = f'{ES_URL}/{es_index}/_mapping/{DOCUMENT_TYPE}'
    log.info("PUT %s", url)
    put_resp = put_json(url, MAPPINGS[DOCUMENT_TYPE])
    check_response(put_resp)

    index_settings = {
        "index": {
            "refresh_interval": collections.current().refresh_interval,
            "max_result_window": collections.current().max_result_window,
        }
    }
    url = f'{ES_URL}/{es_index}/_settings'
    log.info("PUT %s", url)
    put_resp = put_json(url, index_settings)
    check_response(put_resp)


def all_indices():
    indices = requests.get(f'{ES_URL}/_cat/indices?format=json').json()
    return [a['index'] for a in indices if not a['index'].startswith('.monitoring')]


@contextmanager
def snapshot_repo():
    es_index = collections.current().es_index
    id = f'{es_index}-{datetime.utcnow().isoformat().lower()}'
    repo = f'{ES_URL}/_snapshot/{id}'
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


def export_index(stream=None):
    es_index = collections.current().es_index
    with snapshot_repo() as (repo, repo_path):
        snapshot = f'{repo}/{es_index}'
        log.info('Elasticsearch snapshot %r', snapshot)

        log.info('Create snapshot')
        snapshot_resp = put_json(snapshot, {
            'indices': es_index,
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


def import_index(delete=False, stream=None):
    es_index = collections.current().es_index
    if delete:
        delete_index(es_index)

    with snapshot_repo() as (repo, repo_path):
        log.info('Unpack tar archive')

        tar = tarfile.open(mode='r|*', fileobj=stream or sys.stdin.buffer)
        tar.extractall(repo_path)
        tar.close()

        snapshots_resp = requests.get(f'{repo}/*')
        check_response(snapshots_resp)
        for s in snapshots_resp.json()['snapshots']:
            if s['state'] == 'SUCCESS':
                [snapshot_index] = s['indices']
                if snapshot_index != es_index:
                    continue
                snapshot = f'{repo}/{s["snapshot"]}'
                break
        else:
            raise RuntimeError(f"No snapshots for index {es_index}")

        log.info("Starting restore for index %r as %r", snapshot_index, es_index)
        restore = f'{snapshot}/_restore'
        restore_resp = post_json(restore, {
            'indices': es_index,
            'include_global_state': False,
            'include_aliases': False,
        })
        check_response(restore_resp)
        assert restore_resp.json()['accepted']

        status = f'{ES_URL}/{es_index}/_recovery'
        while True:
            status_resp = requests.get(status)
            check_response(status_resp)
            if not status_resp.json():
                log.debug("Waiting for restore to start")
                time.sleep(1)
                continue

            for shard in status_resp.json()[es_index]['shards']:
                stage = shard['stage']
                if stage != 'DONE':
                    log.debug("Shard %r stage=%r", shard['id'], stage)
                    time.sleep(1)
                    break

            else:
                log.info('Snapshot restored successfully')
                break
