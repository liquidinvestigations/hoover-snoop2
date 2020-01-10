import json
import logging
from datetime import datetime
from itertools import chain
from django.conf import settings
import requests
from .utils import zulu

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

ES_URL = settings.SNOOP_STATS_ELASTICSEARCH_URL
ES_INDEX_PREFIX = settings.SNOOP_STATS_ELASTICSEARCH_INDEX_PREFIX
ES_MAPPINGS = {
    'task': {
        'properties': {
            'func': {'type': 'keyword'},
            'args': {'type': 'keyword'},
            'date_created': {'type': 'date', 'format': 'date_time'},
            'date_modified': {'type': 'date', 'format': 'date_time'},
            'date_started': {'type': 'date', 'format': 'date_time'},
            'date_finished': {'type': 'date', 'format': 'date_time'},
            'duration': {'type': 'float'},
        },
    },
    'blob': {
        'properties': {
            'mime_type': {'type': 'keyword'},
            'mime_encoding': {'type': 'keyword'},
            'date_created': {'type': 'date', 'format': 'date_time'},
            'date_modified': {'type': 'date', 'format': 'date_time'},
        },
    },
}


def is_enabled():
    return bool(ES_URL and ES_INDEX_PREFIX)


def reset():
    for document_type in ['task', 'blob']:
        index = ES_INDEX_PREFIX + document_type
        url = f'{ES_URL}/{index}'

        delete_resp = requests.delete(url)
        log.info('%s Elasticsearch DELETE: %r', document_type, delete_resp)

        config = {'mappings': {document_type: ES_MAPPINGS[document_type]}}
        put_resp = requests.put(url, data=json.dumps(config),
                                headers={'Content-Type': 'application/json'})
        log.info('%s Elasticsearch PUT: %r', document_type, put_resp)
        log.info('%s Elasticsearch PUT: %r', document_type, put_resp.text)


def dump(row):
    meta = row._meta
    data = {}
    for f in chain(meta.concrete_fields, meta.private_fields, meta.many_to_many):
        data[f.name] = f.value_from_object(row)

    if data.get('date_finished', None):
        finished = data['date_finished']
        started = data['date_started']
        data['duration'] = (finished - started).total_seconds()

    for k in data:
        if isinstance(data[k], datetime):
            data[k] = zulu(data[k])

    return data


def paginate(iterator, size):
    buffer = []

    for value in iterator:
        buffer.append(value)

        if len(buffer) >= size:
            yield buffer
            buffer = []

    if buffer:
        yield buffer


def bulk_index(row_iter, document_type):
    index = ES_INDEX_PREFIX + document_type
    for row in row_iter:
        address = {
            '_index': index,
            '_type': document_type,
            '_id': row.pk,
        }
        yield {'index': address}
        yield dump(row)


def add_record(row, document_type):
    if not is_enabled():
        return

    log.debug('Sending %s %r', document_type, row)
    index = ES_INDEX_PREFIX + document_type
    resp = requests.put(
        f'{ES_URL}/{index}/{document_type}/{row.pk}',
        data=json.dumps(dump(row)),
        headers={'Content-Type': 'application/json'},
    )

    if not (200 <= resp.status_code < 300):
        log.error('Response: %r', resp)
        log.error('Response text:\n%s', resp.text)
        raise RuntimeError('Put request failed: %r' % resp)


def update():
    from . import models

    if not is_enabled():
        raise RuntimeError("SNOOP_STATS_ELASTICSEARCH_URL or "
                           "SNOOP_STATS_ELASTICSEARCH_INDEX_PREFIX is not set")

    for document_type, model in [('task', models.Task), ('blob', models.Blob)]:
        log.info('Importing table %r ...', document_type)
        queryset = model.objects.all()
        for n, task_list in enumerate(paginate(queryset.iterator(), 1000)):
            log.info('Sending page %d', n + 1)
            lines = (
                json.dumps(m).encode('utf8') + b'\n'
                for m in bulk_index(task_list, document_type)
            )
            resp = requests.post(f'{ES_URL}/_bulk', data=lines,
                                 headers={'Content-Type': 'application/json'})

            if resp.status_code != 200 or resp.json()['errors']:
                log.error('Response: %r', resp)
                log.error('Response text:\n%s', resp.text)
                raise RuntimeError('Bulk request failed: %r' % resp)

        log.info('Done')
