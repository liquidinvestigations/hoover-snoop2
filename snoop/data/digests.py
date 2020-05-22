import logging
import json
import re
import subprocess

from django.conf import settings

from .tasks import shaorma, ShaormaBroken
from . import models
from .utils import zulu
from .analyzers import email
from .analyzers import tika
from .analyzers import exif
from . import ocr
from ._file_types import FILE_TYPES
from . import indexing

log = logging.getLogger(__name__)


def get_collection_langs():
    from .collections import current
    return current().ocr_languages


def is_ocr_mime_type(mime_type):
    return mime_type.startswith('image/') or mime_type == 'application/pdf'


@shaorma('digests.launch', priority=3)
def launch(blob):
    depends_on = {}

    if blob.mime_type == 'message/rfc822':
        depends_on['email_parse'] = email.parse.laterz(blob)

    if tika.can_process(blob):
        depends_on['tika_rmeta'] = tika.rmeta.laterz(blob)

    if exif.can_extract(blob):
        depends_on['exif_data'] = exif.extract.laterz(blob)

    if is_ocr_mime_type(blob.mime_type):
        for lang in get_collection_langs():
            depends_on[f'ocr_{lang}'] = ocr.run_tesseract.laterz(blob, lang)

    gather_task = gather.laterz(blob, depends_on=depends_on)
    index.laterz(blob, depends_on={'digests_gather': gather_task})


@shaorma('digests.gather', priority=8)
def gather(blob, **depends_on):
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
    if is_ocr_mime_type(blob.mime_type):
        for lang in get_collection_langs():
            ocr_blob = depends_on.get(f'ocr_{lang}')
            if not ocr_blob or isinstance(ocr_blob, ShaormaBroken):
                log.warning(f'tesseract ocr result missing for lang {lang}')
                ocr_results[f'tesseract_{lang}'] = ""
                continue
            if ocr_blob.mime_type == 'application/pdf':
                ocr_results[f'tesseract_{lang}'] = \
                    subprocess.check_output(f'pdftotext -enc UTF-8 {ocr_blob.path()} -',
                                            shell=True).decode('utf8')
            else:
                with ocr_blob.open(encoding='utf-8') as f:
                    ocr_results[f'tesseract_{lang}'] = f.read().strip()
    if ocr_results:
        rv['ocr'] = any(len(x.strip()) > 0 for x in ocr_results.values())
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

    _, _ = models.Digest.objects.update_or_create(
        blob=blob,
        defaults=dict(
            result=writer.blob,
        ),
    )

    return writer.blob


@shaorma('digests.index', priority=9)
def index(blob, digests_gather):
    if isinstance(digests_gather, ShaormaBroken):
        raise digests_gather

    digest = models.Digest.objects.get(blob=blob)
    content = _get_document_content(digest)
    version = _get_document_version(digest)
    body = dict(content, _hoover={'version': version})
    try:
        indexing.index(digest.blob.pk, body)
    except RuntimeError:
        log.exception(repr(body))
        raise


def get_filetype(mime_type):
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


def path_parts(path):
    elements = path.split('/')[1:]
    result = []
    prev = None

    for e in elements:
        if prev:
            prev = prev + '/' + e
        else:
            prev = '/' + e

        result.append(prev)

    return result


def directory_id(directory):
    return f'_directory_{directory.pk}'


def parent_id(file):
    parent = file.parent

    if isinstance(parent, models.File):
        return parent.blob.pk

    if isinstance(parent, models.Directory):
        # skip over the dirs that are the children of container files
        if parent.container_file:
            return parent.container_file.blob.pk
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

    header_from = headers.get('From', [''])[0]

    to_domains = [_extract_domain(to) for to in headers_to]
    from_domains = [_extract_domain(header_from)]
    email_domains = to_domains + from_domains

    return {
        'from': header_from,
        'to': list(headers_to),
        'email-domains': [d.lower() for d in email_domains if d],
        'subject': headers.get('Subject', [''])[0],
        'text': '\n\n'.join(text_bits).strip(),
        'pgp': pgp,
        'date': message_date,
    }


email_domain_exp = re.compile("@([\\w.-]+)")


def _extract_domain(text):
    match = email_domain_exp.search(text)
    if match:
        return match[1]


def _get_first_file(digest):
    first_file = (
        digest.blob
        .file_set
        .order_by('pk')
        .first()
    )

    if not first_file:
        first_file = (
            models.File.objects
            .filter(original=digest.blob)
            .order_by('pk')
            .first()
        )

    if not first_file:
        raise RuntimeError(f"Can't find a file for blob {digest.blob}")

    return first_file


def _get_document_content(digest):
    first_file = _get_first_file(digest)

    with digest.result.open() as f:
        digest_data = json.loads(f.read().decode('utf8'))

    attachments = None
    filetype = get_filetype(digest.blob.mime_type)
    if filetype == 'email':
        if first_file.child_directory_set.count() > 0:
            attachments = True

    original = first_file.original
    path = full_path(first_file)

    content = {
        'content-type': original.mime_type,
        'filetype': filetype,
        'text': digest_data.get('text'),
        'pgp': digest_data.get('pgp'),
        'ocr': digest_data.get('ocr'),
        'ocrtext': digest_data.get('ocrtext'),
        'date': digest_data.get('date'),
        'date-created': digest_data.get('date-created'),
        'md5': original.md5,
        'sha1': original.sha1,
        'size': original.size,
        'filename': first_file.name,
        'path': path,
        'path-text': path,
        'path-parts': path_parts(path),
        'broken': digest_data.get('broken'),
        'attachments': attachments,
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
    first_file = _get_first_file(digest)

    children = None
    incomplete_children_list = False
    child_directory = first_file.child_directory_set.first()
    if child_directory:
        children, incomplete_children_list = get_directory_children(child_directory)

    rv = {
        'id': digest.blob.pk,
        'parent_id': parent_id(first_file),
        'has_locations': True,
        'version': _get_document_version(digest),
        'content': _get_document_content(digest),
        'children': children,
        'incomplete_children_list': incomplete_children_list,
    }

    return rv


def get_document_locations(digest):
    def location(file):
        parent_path = full_path(file.parent_directory.container_file or file.parent_directory)
        return {
            'filename': file.name,
            'parent_id': parent_id(file),
            'parent_path': parent_path,
        }

    queryset = digest.blob.file_set.order_by('pk')
    queryset = queryset[:settings.SNOOP_DOCUMENT_LOCATIONS_QUERY_LIMIT + 1]
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
    limit = settings.SNOOP_DOCUMENT_CHILD_QUERY_LIMIT + 1
    child_directory_queryset = directory.child_directory_set.order_by('name_bytes')[:limit]
    child_directory_queryset = child_directory_queryset[:limit]
    child_file_queryset = directory.child_file_set.order_by('name_bytes')
    child_file_queryset = child_file_queryset[:limit]
    incomplete = len(child_directory_queryset) == limit or \
        len(child_file_queryset) == limit
    return (
        [child_dir_to_dict(d) for d in child_directory_queryset][:limit - 1]
        + [child_file_to_dict(f) for f in child_file_queryset][:limit - 1]
    ), incomplete


def get_directory_data(directory):
    children, incomplete_children_list = get_directory_children(directory)
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
        'incomplete_children_list': incomplete_children_list,
    }
