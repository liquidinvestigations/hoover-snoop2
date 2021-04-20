"""Processing pipeline steps for a single document.

After all the files on disk, inside archives, emails and other containers are all ingested and de-duplicated
by the `snoop.data.filesystem` set of tasks, they end up here in the `launch()` Task. Inside this task we
decide what kinds of data and metadata extraction tasks we want to run for the document. We queue them all,
then we queue a `gather()` task that combines their output, and finally we queue the `index()` task to
upload the result into Elasticsearch.

This module also handles generating the different representations for File, Directory and Digest
(de-duplicated document) rows in the database; these are used both in API response generation and when
indexing data into Elasticsearch.
"""

import logging
import json
import subprocess
from collections import OrderedDict
import chardet

from django.conf import settings
from django.core.paginator import Paginator
from django.utils import timezone
from django.db.models import Subquery

from .tasks import snoop_task, SnoopTaskBroken, retry_task, retry_tasks
from . import models
from .utils import zulu, read_exactly
from .analyzers import email
from .analyzers import tika
from .analyzers import exif
from .analyzers import thumbnails
from . import ocr
from ._file_types import FILE_TYPES
from . import indexing
from snoop.data import language_detection

log = logging.getLogger(__name__)
language_detector = language_detection.detectors.get(settings.LANGUAGE_DETECTOR_NAME, None)
ES_MAX_INTEGER = 2 ** 31 - 1


def get_collection_langs():
    """Return the list of OCR languages configured for the current collection."""

    from .collections import current
    return current().ocr_languages


def is_ocr_mime_type(mime_type):
    """Checks if we should run OCR on the given mime type."""

    return mime_type.startswith('image/') or mime_type == 'application/pdf'


@snoop_task('digests.launch', priority=4)
def launch(blob):
    """Task to build and dispatch the different processing tasks for this de-duplicated document.

    Runs [snoop.data.analyzers.email.parse][] on emails, [snoop.data.ocr.run_tesseract][] on OCR-able
    documents, and [snoop.data.analyzers.tika.rmeta][] on compatible documents. Schedules one
    [snoop.data.digests.gather][] Task depending on all of the above to recombine all the results, and
    another [snoop.data.digests.index][] Task depending on the `gather` task.
    """

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

    # launch thumbnail creation
    if settings.SNOOP_THUMBNAIL_URL and thumbnails.can_create(blob):
        depends_on['get_thumbnail'] = (thumbnails.get_thumbnail.laterz(blob))

    gather_task = gather.laterz(blob, depends_on=depends_on, retry=True, delete_extra_deps=True)
    index.laterz(blob, depends_on={'digests_gather': gather_task}, retry=True, queue_now=False)


def can_read_text(blob):
    """Check if document with blob can be read directly to extract text.

    This returns `True` even for `application/octet-stream`, to attempting to extract text from files with
    no mime type found. This sometimes happens for long files.
    """

    EXTRA_TEXT_MIME_TYPES = {
        "application/json",
        "application/octet-stream",
        "application/csv",
        "application/tab-separated-values",
    }
    return blob.mime_type.startswith('text') or \
        (blob.mime_type in EXTRA_TEXT_MIME_TYPES and blob.mime_encoding != 'binary')


def read_text(blob):
    """Attempt to read text from raw text file.

    This function returns a single string, truncated to the `indexing.MAX_TEXT_FIELD_SIZE` constant.

    If provided a file of type "application/octet-stream" (mime type unknown), we attempt to guess encoding
    using "chardet" and abort if we don't see 95% confidence.
    """

    if blob.mime_type == 'application/octet-stream' or blob.mime_encoding == 'binary':
        with blob.open() as f:
            first_4k = read_exactly(f, 4 * 2 ** 10)
        detect_result = chardet.detect(first_4k)
        confidence = detect_result.get('confidence', 0)
        if confidence < 0.8:
            log.warning(f'low confidence when guessing character encoding: {confidence}')
            return
        else:
            encoding = detect_result.get('encoding') or 'latin1'
    else:
        encoding = blob.mime_encoding

    with blob.open(encoding=encoding, errors='replace') as f:
        return read_exactly(f, indexing.MAX_TEXT_FIELD_SIZE, text_mode=True)


def _delete_empty_keys(d):
    """Recursively remove keys from dict that point to empty string, dict, list or None.

    Only values of type dict, str and list are eligible for removal if they have a False value.
    """

    for k in list(d.keys()):
        if isinstance(d[k], dict):
            _delete_empty_keys(d[k])
        if isinstance(d[k], (dict, str, list, type(None))) and not d[k]:
            del d[k]


@snoop_task('digests.gather', priority=7, version=3)
def gather(blob, **depends_on):
    """Combines and serializes the results of the various dependencies into a single
    [snoop.data.models.Digest][] instance.
    """

    rv = {'broken': []}

    # extract text and meta with apache tika
    tika_rmeta_blob = depends_on.get('tika_rmeta')
    if tika_rmeta_blob:
        if isinstance(tika_rmeta_blob, SnoopTaskBroken):
            rv['broken'].append(tika_rmeta_blob.reason)
            log.debug("tika_rmeta task is broken; skipping")

        else:
            with tika_rmeta_blob.open(encoding='utf8') as f:
                tika_rmeta = json.load(f)
            rv['text'] = tika_rmeta[0].get('X-TIKA:content', "")[:indexing.MAX_TEXT_FIELD_SIZE]
            rv['date'] = tika.get_date_modified(tika_rmeta)
            rv['date-created'] = tika.get_date_created(tika_rmeta)
            rv.update(tika.convert_for_indexing(tika_rmeta))

    # parse email for text and headers
    email_parse_blob = depends_on.get('email_parse')
    if email_parse_blob:
        if isinstance(email_parse_blob, SnoopTaskBroken):
            rv['broken'].append(email_parse_blob.reason)
            log.debug("email_parse task is broken; skipping")
        else:
            with email_parse_blob.open(encoding='utf8') as f:
                email_parse = json.load(f)
            rv.update(email.email_meta(email_parse))

    # For large text/CSV files, Tika (and text extraction) fails. For these, we want to read the text
    # directly from the file (limiting by indexing.MAX_TEXT_FIELD_SIZE) and ignore any
    if not rv.get('text') and can_read_text(blob):
        rv['text'] = read_text(blob) or ''

    # combine OCR results, limiting string sizes to indexing.MAX_TEXT_FIELD_SIZE
    ocr_results = dict(ocr.ocr_texts_for_blob(blob))
    if is_ocr_mime_type(blob.mime_type):
        for lang in get_collection_langs():
            ocr_blob = depends_on.get(f'tesseract_{lang}')
            if not ocr_blob or isinstance(ocr_blob, SnoopTaskBroken):
                log.warning(f'tesseract ocr result missing for lang {lang}')
                rv['broken'].append('ocr_missing')
                ocr_results[f'tesseract_{lang}'] = ""
                continue
            if ocr_blob.mime_type == 'application/pdf':
                ocr_results[f'tesseract_{lang}'] = subprocess.check_output(
                    f'pdftotext -q -enc UTF-8 "{ocr_blob.path()}" - | head -c {indexing.MAX_TEXT_FIELD_SIZE}',  # noqa: E501
                    shell=True,
                ).decode('utf8')
            else:
                with ocr_blob.open(encoding='utf-8') as f:
                    ocr_results[f'tesseract_{lang}'] = read_exactly(
                        f,
                        indexing.MAX_TEXT_FIELD_SIZE,
                        text_mode=True,
                    ).strip()
    if ocr_results:
        rv['ocr'] = any(len(x.strip()) > 0 for x in ocr_results.values())
        if rv['ocr']:
            if blob.mime_type == 'application/pdf':
                rv['ocrpdf'] = True
            else:
                rv['ocrimage'] = True
        rv['ocrtext'] = ocr_results

    # combine texts and detect language
    if settings.DETECT_LANGUAGE:
        text = rv.get('text', '')[:2500]
        ocrtexts = [v[:2500] for v in rv.get('ocrtext', {}).values() if v]
        if text or ocrtexts:
            alltext = text + "\n" + "\n".join(ocrtexts)
            try:
                rv['lang'] = language_detector(alltext)
            except Exception as e:
                log.debug(f'Unable to detect language for document {id}: {e}')
                rv['lang'] = None

    # try and extract exif data
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

    rv['has-thumbnails'] = False
    thumbnails = depends_on.get('get_thumbnail')
    if thumbnails:
        if isinstance(thumbnails, SnoopTaskBroken):
            rv['broken'].append(thumbnails.reason)
            log.debug('get_thumbnail task is broken; skipping')
        else:
            rv['has-thumbnails'] = True

    _delete_empty_keys(rv)


    with models.Blob.create() as writer:
        writer.write(json.dumps(rv).encode('utf-8'))

    _, _ = models.Digest.objects.update_or_create(
        blob=blob,
        defaults=dict(
            result=writer.blob,
        ),
    )

    return writer.blob


def _get_tags(digest_id):
    """Helper method to get the document's tags with the correct Elasticsearch field names."""

    if not digest_id:
        return {}

    # add public tags
    q1 = models.DocumentUserTag.objects.filter(digest=digest_id, public=True)
    q1 = q1.values("tag").distinct()
    public_list = list(i['tag'] for i in q1.iterator())
    ret = {indexing.PUBLIC_TAGS_FIELD_NAME: public_list} if public_list else {}

    # add private tags
    q2 = models.DocumentUserTag.objects.filter(digest=digest_id, public=False)
    q2_users = q2.values("user").distinct()
    for u in q2_users.iterator():
        user = u['user']
        tags_for_user = q2.filter(user=user)
        uuid = tags_for_user.first().uuid
        assert uuid != 'invalid'
        private_list = list(i.tag for i in tags_for_user.iterator())
        ret[indexing.PRIVATE_TAGS_FIELD_NAME_PREFIX + uuid] = private_list
    return ret


def _set_tags_timestamps(digest_id, body):
    """Sets 'date-indexed' on all tagas from the body.

    If other tags have been added since digests.index() ran _get_tags() above,
    they shouldn't be in the indexed body and shouldn't be picked up by this function.
    """

    if not digest_id:
        return

    now = timezone.now()

    if indexing.PUBLIC_TAGS_FIELD_NAME in body.keys():
        q = models.DocumentUserTag.objects.filter(
            digest=digest_id,
            public=True,
            tag__in=body[indexing.PUBLIC_TAGS_FIELD_NAME],
            date_indexed__isnull=True,
        )
        q.update(date_indexed=now)

    for key, private_tags in body.items():
        if key.startswith(indexing.PRIVATE_TAGS_FIELD_NAME_PREFIX):
            uuid = key[len(indexing.PRIVATE_TAGS_FIELD_NAME_PREFIX):]
            assert uuid != 'invalid'
            q = models.DocumentUserTag.objects.filter(
                digest=digest_id,
                public=False,
                tag__in=private_tags,
                uuid=uuid,
                date_indexed__isnull=True,
            )
            q.update(date_indexed=now)


@snoop_task('digests.index', priority=8, bulk=True, version=1)
def index(batch):
    """Task used to send a many documents to Elasticsearch.

    End of the processing pipeline for any document.
    """

    # list of (task, body) tuples to send to ES as a single batch request
    result = {}
    documents_to_index = []

    for task in batch:
        blob = task.blob_arg
        first_file = _get_first_file(blob)
        if not first_file:
            log.info("Skipping document with no file: %s", blob)
            result[blob.pk] = False
            continue

        if task.digest_gather_status != models.Task.STATUS_SUCCESS:
            # Generate stub object when gather task is broken (no results).
            # This is needed to find results for which processing has failed.
            digest = None
            content = _get_document_content(None, first_file)
            content.setdefault('broken', []).append('processing_failed')
        else:
            # Generate body from full result set
            digest = models.Digest.objects.get(blob=blob)
            content = _get_document_content(digest)

        if task.tags_count:
            # inject tags at indexing stage, so the private ones won't get spilled in
            # the document/file endpoints
            content.update(_get_tags(task.digest_id))

        version = _get_document_version(digest)
        body = dict(content, _hoover={'version': version})

        # es 6.8 "integer" has max size 2^31-1
        # and we managed to set "size" as an "integer" field
        # instead of a long field
        size = body.get('size', 0)
        if size > ES_MAX_INTEGER:
            body['size'] = ES_MAX_INTEGER

        documents_to_index.append((task, body))

    rv = indexing.bulk_index([(task.blob_arg.pk, body) for task, body in documents_to_index])
    for x in rv['items']:
        blob = x['index']['_id']
        ok = 200 <= x['index']['status'] < 300
        result[blob] = ok

    for task, body in documents_to_index:
        if task.tags_count:
            _set_tags_timestamps(task.digest_id, body)

    return result


def retry_index(blob):
    """Retry the [snoop.data.digests.index][] Task for the given Blob.

    Needed by the web process when some user changes the Document tags; this function will be called for the
    affected document."""
    try:
        task = models.Task.objects.filter(func='digests.index', blob_arg=blob).get()
        if task.status == models.Task.STATUS_PENDING:
            return
        retry_task(task)
    except Exception as e:
        log.exception(e)


def update_all_tags():
    """Re-runs the index task for all tags that have not been indexed.

    Only works on tasks that are not already PENDING.

    Requires a collection to be selected.
    """

    tags = models.DocumentUserTag.objects.filter(
        date_indexed__isnull=True,
    )
    digests = tags.distinct('digest').values('digest__blob')
    tasks = models.Task.objects.filter(
        func='digests.index',
        blob_arg__pk__in=Subquery(digests),
    )
    tasks = tasks.exclude(status=models.Task.STATUS_PENDING)
    retry_tasks(tasks)


def get_filetype(mime_type):
    """Returns a Hoover-specific "file type" derived from the `libmagic` mime type.

    See [snoop.data._file_types.FILE_TYPES][] for extended list. Extra patterns like `audio/* -> audio` are
    applied in this file.
    """

    if mime_type in FILE_TYPES:
        return FILE_TYPES[mime_type]

    supertype = mime_type.split('/')[0]
    if supertype in ['audio', 'video', 'image']:
        return supertype

    return None


def full_path(file):
    """Returns full path of File or Directory, relative to collection root.

    `//` is used to mark files from inside containers (archives). This happens naturally when iterating
    objects, since all container files will contain a single directory with `name = ''`. See
    [snoop.data.models.Directory.container_file][] for more details.
    """
    node = file
    elements = [file.name]
    while node.parent:
        node = node.parent
        elements.append(node.name)
    return '/'.join(reversed(elements))


def path_parts(path):
    """Returns a list of all the prefixes for this document's [snoop.data.digests.full_path][].

    This is set on the Digest as field `path-parts` to create path buckets in Elasticsearch.
    """

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
    """Returns an ID of the form `_directory_$ID` to represent a [snoop.data.models.Directory][].

    This ID is used to cross-link objects in the API.
    """

    return f'_directory_{directory.pk}'


def file_id(file_query):
    """Returns an ID of the form `_file_$ID` to represent a [snoop.data.models.File][].

    This ID is used to cross-link objects in the API.
    """

    return f'_file_{file_query.pk}'


def _get_parent(item):
    """Returns the parent of the File or Directory, skipping the Directory directly under an archive."""

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
    """Returns the ID of the parent entity, for linking in the API."""

    parent = _get_parent(item)

    if isinstance(parent, models.File):
        return file_id(parent)

    if isinstance(parent, models.Directory):
        return directory_id(parent)

    return None


def parent_children_page(item):
    """Return the page number on the parent that points to the page containing this item.

    All items have a `children` list in their doc API. That list is paginated by page number.
    When fetching an item from the middle of the tree, we need to populate the list of siblings. Since the
    view is paginated, all parent objects must select the correct page in order find the item
    in its parent's child list.

    See [snoop.data.digests.get_directory_children][] on how this list is created.
    """

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


def _get_first_file(blob):
    """Returns first file pointing to this Blob, ordered by file ID."""

    first_file = (
        blob
        .file_set
        .order_by('pk')
        .first()
    )

    if not first_file:
        first_file = (
            models.File.objects
            .filter(original=blob)
            .order_by('pk')
            .first()
        )

    return first_file


def _get_document_content(digest, the_file=None):
    """Helper method converts Digest data into dict with content data.

    Fields are selected from the [snoop.data.models.Digest][] object and combined with an optional
    [snoop.data.models.File][].

    This data is returned under the `content` key by [snoop.data.digests.get_document_data][] and
    [snoop.data.digests.get_file_data][], the functions that return API data for those respective endpoints.

    This data is also used directly as the data to index in [snoop.data.digests.index][].

    Since the data here is served for anyone with access to the collection, private user data can't be added
    here.
    """

    def get_text_lengths(data):
        yield len(data.get('text', ''))
        for k in data.get('ocrtext', {}).values():
            yield len(k or '')

    def get_word_count(data):
        return max(get_text_lengths(data))

    if not the_file:
        the_file = _get_first_file(digest.blob)

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

    content = OrderedDict({
        'content-type': original.mime_type,
        'filetype': filetype,
        'md5': original.md5,
        'sha1': original.sha1,
        'id': original.sha3_256,
        'size': original.size,
        'filename': the_file.name,
        'path': path,
        'path-text': path,
        'path-parts': path_parts(path),
        'attachments': attachments,
    })

    content.update(digest_data)
    content['word-count'] = get_word_count(content)

    # delete old "email" field that may be left behind on older digest data.
    if 'email' in content:
        del content['email']

    # for missing "digest" objects, we mark this as a separate (more general) reason
    if not digest_data:
        content.setdefault('broken', []).append('processing_failed')

    return content


def _get_document_version(digest):
    """The document version is the date of indexing in ISO format."""

    if not digest:
        return None
    return zulu(digest.date_modified)


def get_document_data(blob, children_page=1):
    """Returns dict with representation of de-duplicated document ([snoop.data.models.Digest][])."""

    first_file = _get_first_file(blob)

    children = None
    has_next = False
    total = 0
    pages = 0
    child_directory = first_file.child_directory_set.first()
    if child_directory:
        children, has_next, total, pages = get_directory_children(child_directory, children_page)

    try:
        digest = models.Digest.objects.get(blob=blob)
    except models.Digest.DoesNotExist:
        digest = None

    rv = {
        'id': blob.pk,
        'parent_id': parent_id(first_file),
        'has_locations': True,
        'version': _get_document_version(digest),
        'content': _get_document_content(digest, first_file),
        'children': children,
        'children_page': children_page,
        'children_has_next_page': has_next,
        'children_count': total,
        'children_page_count': pages,
        'parent_children_page': parent_children_page(first_file),
    }

    return rv


def get_document_locations(digest, page_index):
    """Returns list of dicts representing a page of locations for a given document."""

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
        'size': file.size,
    }


def child_dir_to_dict(directory):
    return {
        'id': directory_id(directory),
        'content_type': 'application/x-directory',
        'filetype': 'folder',
        'filename': directory.name,
    }


def get_directory_children(directory, page_index=1):
    """Returns a list with the page of children for a given directory.

    This list combines both File children and Directory children into a single view. The first pages are of
    Directories, and the following pages contain only Files. There's a page in the middle that contains both
    some Directories and some Files.

    See [snoop.data.digests.parent_children_page][] for computing the page number for an item inside this
    list.
    """

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
    """Returns dict with representation of a [snoop.data.models.Directory][]."""

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
    """Returns dict with representation of a [snoop.data.models.File][]."""

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
