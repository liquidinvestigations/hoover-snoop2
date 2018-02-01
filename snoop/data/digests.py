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
        rv['text'] = tika_rmeta[0].get('X-TIKA:content', "")

    email_parse_blob = depends_on.get('email_parse')
    if email_parse_blob:
        with email_parse_blob.open(encoding='utf8') as f:
            email_parse = json.load(f)
        rv['email'] = email_parse

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

    email_data = digest_data['email']
    headers = email_data['headers']

    text_bits = []
    for part in iter_parts(email_data):
        part_text = part.get('text')
        if part_text:
            text_bits.append(part_text)

    headers_to = set()
    for header in ['To', 'Cc', 'Bcc', 'Resent-To', 'Recent-Cc']:
        headers_to.update(headers.get(header, []))

    return {
        'from': headers.get('From', [''])[0],
        'to': list(headers_to),
        'subject': headers.get('Subject', [''])[0],
        'text': '\n\n'.join(text_bits).strip(),
    }


def get_document_data(digest):
    with digest.result.open() as f:
        digest_data = json.loads(f.read().decode('utf8'))

    blob = digest.blob
    first_file = blob.file_set.order_by('pk').first()
    content = {
        'content-type': blob.mime_type,
        'filetype': filetype(blob.mime_type),
        'text': digest_data.get('text'),
        'md5': blob.md5,
        'sha1': blob.sha1,
        'size': blob.path().stat().st_size,
        'filename': first_file.name,
        'path': full_path(first_file),
    }

    children = None

    if blob.mime_type == 'message/rfc822':
        content.update(email_meta(digest_data))

        attachments = first_file.child_directory_set.first()
        if attachments:
            children = [
                child_file_to_dict(f)
                for f in attachments.child_file_set.order_by('pk').all()
            ]

    text = content.get('text') or ""
    content['word-count'] = len(text.strip().split())

    rv = {
        'id': blob.pk,
        'parent_id': parent_id(first_file),
        'has_locations': True,
        'version': zulu(digest.date_modified),
        'content': content,
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


def get_directory_data(directory):
    children = (
        [child_dir_to_dict(d) for d in directory.child_directory_set.all()] +
        [child_file_to_dict(f) for f in directory.child_file_set.all()]
    )

    return {
        'id': directory_id(directory),
        'parent_id': parent_id(directory),
        'content': {
            'content-type': 'application/x-directory',
            'filetype': 'folder',
            'filename': directory.name,
            'path': full_path(directory),
        },
        'children': children,
    }
