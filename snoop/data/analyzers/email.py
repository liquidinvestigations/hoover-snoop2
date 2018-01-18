import json
from collections import defaultdict
import email
from .. import models
from ..tasks import shaorma


def iter_parts(message, numbers=[]):
    if message.is_multipart():
        for n, part in enumerate(message.get_payload(), 1):
            yield from iter_parts(part, numbers + [str(n)])
    else:
        yield '.'.join(numbers), message


def get_headers(message):
    rv = defaultdict(list)

    for key in message.keys():
        for header in message.get_all(key):
            rv[key.title()].append(header)

    return dict(rv)


@shaorma
def parse(blob_pk):
    blob = models.Blob.objects.get(pk=blob_pk)
    with blob.open() as f:
        message = email.message_from_bytes(f.read())

    data = {
        'headers': get_headers(message),
    }

    with models.Blob.create() as output:
        output.write(json.dumps(data, indent=2).encode('utf8'))

    return output.blob
