import logging
import json
import re
import subprocess

from django.conf import settings
from django.core.paginator import Paginator

from .tasks import snoop_task, SnoopTaskBroken
from . import models
from .utils import zulu
from .analyzers import email
from .analyzers import tika
from .analyzers import exif
from . import ocr
from ._file_types import FILE_TYPES
from . import indexing

log = logging.getLogger(__name__)
ES_MAX_INTEGER = 2 ** 31 - 1


def get_collection_langs():
    from .collections import current
    return current().ocr_languages


def is_ocr_mime_type(mime_type):
    return mime_type.startswith('image/') or mime_type == 'application/pdf'


@snoop_task('digests.launch', priority=4)
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
            depends_on[f'tesseract_{lang}'] = ocr.run_tesseract.laterz(blob, lang)

    gather_task = gather.laterz(blob, depends_on=depends_on, retry=True)
    index.laterz(blob, depends_on={'digests_gather': gather_task}, retry=True, queue_now=False)


@snoop_task('digests.gather', priority=7)
def gather(blob, **depends_on):
    rv = {'broken': []}
    text_blob = depends_on.get('text')
    if text_blob:
        with text_blob.open() as f:
            text_bytes = f.read()
        encoding = 'latin1' if blob.mime_encoding == 'binary' else blob.mime_encoding
        rv['text'] = text_bytes.decode(encoding)

    tika_rmeta_blob = depends_on.get('tika_rmeta')
    if tika_rmeta_blob:
        if isinstance(tika_rmeta_blob, SnoopTaskBroken):
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
        if isinstance(email_parse_blob, SnoopTaskBroken):
            rv['broken'].append(email_parse_blob.reason)
            log.debug("email_parse task is broken; skipping")

        else:
            with email_parse_blob.open(encoding='utf8') as f:
                email_parse = json.load(f)
            rv['email'] = email_parse

    ocr_results = dict(ocr.ocr_texts_for_blob(blob))
    if is_ocr_mime_type(blob.mime_type):
        for lang in get_collection_langs():
            ocr_blob = depends_on.get(f'tesseract_{lang}')
            if not ocr_blob or isinstance(ocr_blob, SnoopTaskBroken):
                log.warning(f'tesseract ocr result missing for lang {lang}')
                ocr_results[f'tesseract_{lang}'] = ""
                continue
            if ocr_blob.mime_type == 'application/pdf':
                ocr_results[f'tesseract_{lang}'] = \
                    subprocess.check_output(f'pdftotext -q -enc UTF-8 "{ocr_blob.path()}" -',
                                            shell=True).decode('utf8')
            else:
                with ocr_blob.open(encoding='utf-8') as f:
                    ocr_results[f'tesseract_{lang}'] = f.read().strip()
    if ocr_results:
        rv['ocr'] = any(len(x.strip()) > 0 for x in ocr_results.values())
        if rv['ocr']:
            if blob.mime_type == 'application/pdf':
                rv['ocrpdf'] = True
            else:
                rv['ocrimage'] = True
        rv['ocrtext'] = ocr_results

    exif_data_blob = depends_on.get('exif_data')
    if exif_data_blob:
        if isinstance(exif_data_blob, SnoopTaskBroken):
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


@snoop_task('digests.index', priority=8)
def index(blob, digests_gather):
    if isinstance(digests_gather, SnoopTaskBroken):
        raise digests_gather

    digest = models.Digest.objects.get(blob=blob)
    content = _get_document_content(digest)
    version = _get_document_version(digest)
    body = dict(content, _hoover={'version': version})

    # es 6.8 "integer" has max size 2^31-1
    # and we managed to set "size" as an "integer" field
    # instead of a long field
    size = body.get('size', 0)
    if size > ES_MAX_INTEGER:
        body['size'] = ES_MAX_INTEGER

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


def file_id(file_query):
    return f'_file_{file_query.pk}'


def _get_parent(item):
    parent = item.parent

    if isinstance(parent, models.File):
        return parent

    if isinstance(parent, models.Directory):
        # skip over the dirs that are the children of container files
        if parent.container_file:
            return parent.container_file
        return parent

    return None


def parent_id(item):
    parent = _get_parent(item)

    if isinstance(parent, models.File):
        return file_id(parent)

    if isinstance(parent, models.Directory):
        return directory_id(parent)

    return None


def parent_children_page(item):
    # don't use _get_parent --> don't skip parents when for polling children
    parent = item.parent
    if isinstance(item, models.Directory) and item.container_file:  # dummy archive directory
        return 1
    if not parent:  # root document, no parent
        return 1
    assert isinstance(parent, models.Directory)

    page_index = 1
    if isinstance(item, models.File):
        children = parent.child_file_set
        dir_paginator = Paginator(parent.child_directory_set,
                                  settings.SNOOP_DOCUMENT_LOCATIONS_QUERY_LIMIT)
        dir_pages = dir_paginator.num_pages
        page_index += dir_pages
        # last page of dirs also contains first page of files
        if dir_pages:
            page_index -= 1

    if isinstance(item, models.Directory):
        children = parent.child_directory_set

    children_before_item = children.filter(name_bytes__lt=item.name_bytes).count()
    page_index += int(children_before_item / settings.SNOOP_DOCUMENT_CHILD_QUERY_LIMIT)

    return page_index


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


def _get_document_content(digest, the_file=None):
    if not the_file:
        the_file = _get_first_file(digest)

    digest_data = {}
    if digest is not None:
        with digest.result.open() as f:
            digest_data = json.loads(f.read().decode('utf8'))

    attachments = None
    filetype = get_filetype(the_file.blob.mime_type)
    if filetype == 'email':
        if the_file.child_directory_set.count() > 0:
            attachments = True

    original = the_file.original
    path = full_path(the_file)

    content = {
        'content-type': original.mime_type,
        'filetype': filetype,
        'text': digest_data.get('text'),
        'pgp': digest_data.get('pgp'),
        'ocr': digest_data.get('ocr'),
        'ocrtext': digest_data.get('ocrtext'),
        'ocrpdf': digest_data.get('ocrpdf'),
        'ocrimage': digest_data.get('ocrimage'),
        'date': digest_data.get('date'),
        'date-created': digest_data.get('date-created'),
        # TODO 7zip, unzip, all of these will list the correct access/creation
        # times when listing, but don't preserve them when unpacking.
        # 'date': digest_data.get('date') or zulu(the_file.mtime),
        # 'date-created': digest_data.get('date-created') or zulu(the_file.ctime),
        'md5': original.md5,
        'sha1': original.sha1,
        'size': original.size,
        'filename': the_file.name,
        'path': path,
        'path-text': path,
        'path-parts': path_parts(path),
        'broken': digest_data.get('broken'),
        'attachments': attachments,
    }

    if the_file.blob.mime_type == 'message/rfc822':
        content.update(email_meta(digest_data))

    if 'location' in digest_data:
        content['location'] = digest_data['location']

    text = content.get('text') or ""
    content['word-count'] = len(text.strip().split())

    return content


def _get_document_version(digest):
    return zulu(digest.date_modified)


def get_document_data(digest, children_page=1):
    first_file = _get_first_file(digest)

    children = None
    has_next = False
    total = 0
    pages = 0
    child_directory = first_file.child_directory_set.first()
    if child_directory:
        children, has_next, total, pages = get_directory_children(child_directory, children_page)

    rv = {
        'id': digest.blob.pk,
        'parent_id': parent_id(first_file),
        'has_locations': True,
        'version': _get_document_version(digest),
        'content': _get_document_content(digest),
        'children': children,
        'children_page': children_page,
        'children_has_next_page': has_next,
        'children_count': total,
        'children_page_count': pages,
        'parent_children_page': parent_children_page(first_file),
    }

    return rv


def get_document_locations(digest, page_index):

    def location(file):
        parent_path = full_path(file.parent_directory.container_file or file.parent_directory)
        return {
            'id': file_id(file),
            'filename': file.name,
            'parent_id': parent_id(file),
            'parent_path': parent_path,
        }

    queryset = digest.blob.file_set.order_by('pk')
    paginator = Paginator(queryset, settings.SNOOP_DOCUMENT_LOCATIONS_QUERY_LIMIT)
    page = paginator.page(page_index)

    return [location(file) for file in page.object_list], page.has_next()


def child_file_to_dict(file):
    blob = file.blob
    return {
        # 'id': blob.pk,
        'id': file_id(file),
        'file': file_id(file),
        'digest': blob.pk,
        'content_type': blob.mime_type,
        'filetype': get_filetype(blob.mime_type),
        'filename': file.name,
    }


def child_dir_to_dict(directory):
    return {
        'id': directory_id(directory),
        'content_type': 'application/x-directory',
        'filetype': 'folder',
        'filename': directory.name,
    }


def get_directory_children(directory, page_index=1):
    def get_list(p1, p1f, p2, p2f, idx):
        if idx < p1.num_pages:
            return [p1f(x) for x in p1.page(idx).object_list]

        # last page of dirs continues with first page of files
        if idx == p1.num_pages:
            return ([p1f(x) for x in p1.page(idx).object_list]
                    + [p2f(x) for x in p2.page(1).object_list])

        # skip the 1 page we added above
        idx -= p1.num_pages - 1

        if idx <= p2.num_pages:
            return [p2f(x) for x in p2.page(idx).object_list]
        return []

    def has_next(p1, p2, page_index):
        return page_index < (p1.num_pages + p2.num_pages - 1)

    limit = settings.SNOOP_DOCUMENT_CHILD_QUERY_LIMIT
    child_directory_queryset = directory.child_directory_set.order_by('name_bytes')
    child_file_queryset = directory.child_file_set.order_by('name_bytes')
    p1 = Paginator(child_directory_queryset, limit)
    p1f = child_dir_to_dict
    p2 = Paginator(child_file_queryset, limit)
    p2f = child_file_to_dict
    pages = p1.num_pages + p2.num_pages - 1
    assert page_index > 0
    assert page_index <= pages

    total = child_directory_queryset.count() + child_file_queryset.count()
    return get_list(p1, p1f, p2, p2f, page_index), \
        has_next(p1, p2, page_index), total, pages


def get_directory_data(directory, children_page=1):
    children, has_next, total, pages = get_directory_children(directory, children_page)
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
        'children_page': children_page,
        'children_has_next_page': has_next,
        'children_count': total,
        'children_page_count': pages,
        'parent_children_page': parent_children_page(directory),
    }


def get_file_data(file, children_page=1):
    children = None
    has_next = False
    total = 0
    pages = 0
    child_directory = file.child_directory_set.first()
    if child_directory:
        children, has_next, total, pages = get_directory_children(child_directory, children_page)

    blob = file.blob

    digest = None
    version = None
    content = None
    try:
        digest = blob.digest
        version = _get_document_version(digest)
        content = _get_document_content(digest, file)
    except models.Blob.digest.RelatedObjectDoesNotExist:
        version = _get_document_version(file)
        content = _get_document_content(None, file)

    rv = {
        'id': file_id(file),
        'digest': blob.pk,
        'parent_id': parent_id(file),
        'has_locations': True,
        'version': version,
        'content': content,
        'children': children,
        'children_page': children_page,
        'children_has_next_page': has_next,
        'children_count': total,
        'children_page_count': pages,
        'parent_children_page': parent_children_page(file),
    }

    return rv
