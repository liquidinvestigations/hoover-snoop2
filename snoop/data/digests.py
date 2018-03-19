import logging
import json
from .tasks import shaorma, ShaormaBroken
from . import models
from .utils import zulu
from .analyzers import email
from .analyzers import tika
from .analyzers import exif
from ._file_types import FILE_TYPES
from . import ocr
from . import indexing

log = logging.getLogger(__name__)


@shaorma('digests.launch')
def launch(blob, collection_pk):
    depends_on = {}

    if blob.mime_type == 'message/rfc822':
        depends_on['email_parse'] = email.parse.laterz(blob)

    if tika.can_process(blob):
        depends_on['tika_rmeta'] = tika.rmeta.laterz(blob)

    if exif.can_extract(blob):
        depends_on['exif_data'] = exif.extract.laterz(blob)

    gather_task = gather.laterz(blob, collection_pk, depends_on=depends_on)
    index.laterz(blob, collection_pk, depends_on={'digests_gather': gather_task})


@shaorma('digests.gather')
def gather(blob, collection_pk, **depends_on):
    collection = models.Collection.objects.get(pk=collection_pk)

    rv = {'broken': []}
    text_blob = depends_on.get('text')
    if text_blob:
        with text_blob.open() as f:
            text_bytes = f.read()
        rv['text'] = text_bytes.decode(text_blob.mime_encoding)

    tika_rmeta_blob = depends_on.get('tika_rmeta')
    if tika_rmeta_blob:
        if isinstance(tika_rmeta_blob, ShaormaBroken):
            rv['broken'].append(tika_rmeta_blob.reason)
            log.debug("tika_rmeta task is broken; skipping")

        else:
            with tika_rmeta_blob.open(encoding='utf8') as f:
                tika_rmeta = json.load(f)
            rv['text'] = tika_rmeta[0].get('X-TIKA:content', "")
            rv['date'] = tika.get_date_modified(tika_rmeta)
            rv['date-created'] = tika.get_date_created(tika_rmeta)

    email_parse_blob = depends_on.get('email_parse')
    if email_parse_blob:
        if isinstance(email_parse_blob, ShaormaBroken):
            rv['broken'].append(email_parse_blob.reason)
            log.debug("email_parse task is broken; skipping")

        else:
            with email_parse_blob.open(encoding='utf8') as f:
                email_parse = json.load(f)
            rv['email'] = email_parse

    ocr_results = dict(ocr.ocr_texts_for_blob(blob))
    if ocr_results:
        text = rv.get('text', "")
        for _, ocr_text in sorted(ocr_results.items()):
            text += ' ' + ocr_text
        rv['text'] = text
        rv['ocr'] = True
        rv['ocrtext'] = ocr_results

    exif_data_blob = depends_on.get('exif_data')
    if exif_data_blob:
        if isinstance(exif_data_blob, ShaormaBroken):
            rv['broken'].append(exif_data_blob.reason)
            log.debug("exif_data task is broken; skipping")

        else:
            with exif_data_blob.open(encoding='utf8') as f:
                exif_data = json.load(f)
            rv['location'] = exif_data.get('location')
            rv['date-created'] = exif_data.get('date-created')

    with models.Blob.create() as writer:
        writer.write(json.dumps(rv).encode('utf-8'))

    digest, _ = collection.digest_set.update_or_create(
        blob=blob,
        defaults=dict(
            result=writer.blob,
        ),
    )

    return writer.blob

@shaorma('digests.index')
def index(blob, collection_pk, digests_gather):
    digest = models.Digest.objects.get(blob=blob, collection__pk=collection_pk)
    content = _get_document_content(digest)
    version = _get_document_version(digest)
    body = dict(content, _hoover={'version': version})
    indexing.index(
        digest.collection.name,
        digest.blob.pk,
        body,
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


def directory_id(directory):
    return f'_directory_{directory.pk}'


def parent_id(file):
    parent = file.parent

    if isinstance(parent, models.File):
        return parent.blob.pk

    if isinstance(parent, models.Directory):
        return directory_id(parent)

    return None


def email_meta(digest_data):
    def iter_parts(email_data):
        yield email_data
        for part in email_data.get('parts') or []:
            yield from iter_parts(part)

    email_data = digest_data.get('email')
    if not email_data:
        return {}

    headers = email_data['headers']

    text_bits = []
    pgp = False
    for part in iter_parts(email_data):
        part_text = part.get('text')
        if part_text:
            text_bits.append(part_text)

        if part.get('pgp'):
            pgp = True

    headers_to = set()
    for header in ['To', 'Cc', 'Bcc', 'Resent-To', 'Recent-Cc']:
        headers_to.update(headers.get(header, []))

    message_date = None
    message_raw_date = headers.get('Date', [None])[0]
    if message_raw_date:
        message_date = zulu(email.parse_date(message_raw_date))

    return {
        'from': headers.get('From', [''])[0],
        'to': list(headers_to),
        'subject': headers.get('Subject', [''])[0],
        'text': '\n\n'.join(text_bits).strip(),
        'pgp': pgp,
        'date': message_date,
    }


def _get_document_content(digest):
    first_file = digest.blob.file_set.order_by('pk').first()

    with digest.result.open() as f:
        digest_data = json.loads(f.read().decode('utf8'))

    content = {
        'content-type': digest.blob.mime_type,
        'filetype': filetype(digest.blob.mime_type),
        'text': digest_data.get('text'),
        'pgp': digest_data.get('pgp'),
        'ocr': digest_data.get('ocr'),
        'ocrtext': digest_data.get('ocrtext'),
        'date': digest_data.get('date'),
        'date-created': digest_data.get('date-created'),
        'md5': digest.blob.md5,
        'sha1': digest.blob.sha1,
        'size': digest.blob.size,
        'filename': first_file.name,
        'path': full_path(first_file),
        'broken': digest_data.get('broken'),
    }

    if digest.blob.mime_type == 'message/rfc822':
        content.update(email_meta(digest_data))

    if 'location' in digest_data:
        content['location'] = digest_data['location']

    text = content.get('text') or ""
    content['word-count'] = len(text.strip().split())

    return content


def _get_document_version(digest):
    return zulu(digest.date_modified)


def get_document_data(digest):
    first_file = digest.blob.file_set.order_by('pk').first()

    children = None
    child_directory = first_file.child_directory_set.first()
    if child_directory:
        children = get_directory_children(child_directory)

    rv = {
        'id': digest.blob.pk,
        'parent_id': parent_id(first_file),
        'has_locations': True,
        'version': _get_document_version(digest),
        'content': _get_document_content(digest),
        'children': children,
    }

    return rv


def get_document_locations(digest):
    def location(file):
        parent = file.parent_directory
        return {
            'filename': file.name,
            'parent_id': directory_id(parent),
            'parent_path': full_path(parent),
        }

    queryset = digest.blob.file_set.order_by('pk')
    return [location(file) for file in queryset]


def child_file_to_dict(file):
    blob = file.blob
    return {
        'id': blob.pk,
        'content_type': blob.mime_type,
        'filename': file.name,
    }


def child_dir_to_dict(directory):
    return {
        'id': directory_id(directory),
        'content_type': 'application/x-directory',
        'filename': directory.name,
    }


def get_directory_children(directory):
    child_directory_queryset = directory.child_directory_set.order_by('name')
    child_file_queryset = directory.child_file_set.order_by('name')
    return (
        [child_dir_to_dict(d) for d in child_directory_queryset] +
        [child_file_to_dict(f) for f in child_file_queryset]
    )


def get_directory_data(directory):
    return {
        'id': directory_id(directory),
        'parent_id': parent_id(directory),
        'content': {
            'content-type': 'application/x-directory',
            'filetype': 'folder',
            'filename': directory.name,
            'path': full_path(directory),
        },
        'children': get_directory_children(directory),
    }
