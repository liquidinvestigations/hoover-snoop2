import json
import logging
from datetime import datetime
from django.forms.models import model_to_dict
from django.conf import settings
import requests
from . import models

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

ES_URL = settings.SNOOP_ELASTICSEARCH_URL
ES_INDEX = settings.SNOOP_ELASTICSEARCH_INDEX
ES_MAPPINGS = {
    'task': {
        'properties': {
            'func': {'type': 'string', 'index': 'not_analyzed'},
            #'args': {'type': 'keyword'},  # TODO needs ES5
            'args': {'type': 'string', 'index': 'not_analyzed'},
            'date_started': {'type': 'date', 'index': 'not_analyzed'},
            'date_finished': {'type': 'date', 'index': 'not_analyzed'},
        },
    },
}


def reset():
    url = f'{ES_URL}/{ES_INDEX}'

    delete_resp = requests.delete(url)
    log.info('Elasticsearch DELETE: %r', delete_resp)

    config = {'mappings': ES_MAPPINGS}
    put_resp = requests.put(url, data=json.dumps(config))
    log.info('Elasticsearch PUT: %r', put_resp)
    log.info('Elasticsearch PUT: %r', put_resp.text)


def dump(row):
    data = model_to_dict(row)

    for k in data:
        if isinstance(data[k], datetime):
            data[k] = data[k].isoformat()

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
    for row in row_iter:
        address = {
            '_index': ES_INDEX,
            '_type': document_type,
            '_id': row.pk,
        }
        yield {'index': address}
        yield dump(row)


def update():
    queryset = models.Task.objects.all()
    for n, task_list in enumerate(paginate(queryset.iterator(), 1000)):
        log.info('Sending page %d', n + 1)
        lines = (
            json.dumps(m).encode('utf8') + b'\n'
            for m in bulk_index(task_list, 'task')
        )
        resp = requests.post(f'{ES_URL}/_bulk', data=lines)

        if resp.status_code != 200 or resp.json()['errors']:
            log.error('Response: %r', resp)
            log.error('Response text:\n%s', resp.text)
            raise RuntimeError('Bulk request failed: %r' % resp)

    log.info('done')
