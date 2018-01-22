import json
from .tasks import shaorma
from . import models
from .utils import zulu
from .analyzers import email
from .analyzers import tika
from ._file_types import FILE_TYPES


@shaorma('digests.launch')
def launch(blob, collection_pk):
    depends_on = {}

    if blob.mime_type == 'message/rfc822':
        depends_on['email_parse'] = email.parse.laterz(blob)

    if tika.can_process(blob):
        depends_on['tika_rmeta'] = tika.rmeta.laterz(blob)

    gather.laterz(blob, collection_pk, depends_on=depends_on)


@shaorma('digests.gather')
def gather(blob, collection_pk, **depends_on):
    collection = models.Collection.objects.get(pk=collection_pk)

    rv = {}
    text_blob = depends_on.get('text')
    if text_blob:
        with text_blob.open() as f:
            text_bytes = f.read()
        rv['text'] = text_bytes.decode(text_blob.mime_encoding)

    tika_rmeta_blob = depends_on.get('tika_rmeta')
    if tika_rmeta_blob:
        with tika_rmeta_blob.open(encoding='utf8') as f:
            tika_rmeta = json.load(f)
        rv['text'] = tika_rmeta[0]['X-TIKA:content']

    email_parse_blob = depends_on.get('email_parse')
    if email_parse_blob:
        with email_parse_blob.open(encoding='utf8') as f:
            email_parse = json.load(f)
        rv['_emailheaders'] = email_parse['headers']

    with models.Blob.create() as writer:
        writer.write(json.dumps(rv).encode('utf-8'))

    collection.digest_set.update_or_create(
        blob=blob,
        defaults=dict(
            result=writer.blob,
        ),
    )


def filetype(mime_type):
    if mime_type in FILE_TYPES:
        return FILE_TYPES[mime_type]

    supertype = mime_type.split('/')[0]
    if supertype in ['audio', 'video', 'image']:
        return supertype

    return None


def full_path(file):
    node = file
    elements = [file.name]
    while node.parent:
        node = node.parent
        elements.append(node.name)
    return '/'.join(reversed(elements))


def get_document_data(digest):
    with digest.result.open() as f:
        digest_data = json.loads(f.read().decode('utf8'))

    first_file = digest.blob.file_set.order_by('pk').first()

    return {
        'id': digest.blob.pk,
        'version': zulu(digest.date_modified),
        'content': {
            'content-type': digest.blob.mime_type,
            'filetype': filetype(digest.blob.mime_type),
            'text': digest_data.get('text'),
            'md5': digest.blob.md5,
            'sha1': digest.blob.sha1,
            'size': digest.blob.path().stat().st_size,
            'filename': first_file.name,
            'path': full_path(first_file),
            '_emailheaders': digest_data.get('_emailheaders'),
        },
    }
