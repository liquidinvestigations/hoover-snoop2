"""Task definitions for ingesting and running OCR.

OCR results can be imported from an external source (supplied on disk) or through running Tesseract directly
on the workers. The different tasks defined here implement these two methods of obtaining OCR results.

Identifying OCR results with documents is very simple: for external OCR we use the MD5 (which is required to
be a part of the filename of the files on disk), and for the OCR we run ourselves we use a Task dependency
(that internally uses the sha3_256 of the document content as the primary key).
"""

import json
import logging
import multiprocessing
import os
import re
import string
import subprocess
import tempfile

from django.conf import settings
from . import models
from .tasks import snoop_task, require_dependency, retry_tasks, SnoopTaskBroken
from .analyzers import tika


TESSERACT_OCR_IMAGE_MIME_TYPES = {
    'image/jpeg',
    'image/png',
    'image/tiff',
    'image/bmp',
    'image/gif',
    'image/webp',
    'image/x-portable-anymap',
    'image/jp2',
}
"""Mime types of images formats supported by tesseracts OCR.

Tesseract uses the [leptonica](https://github.com/DanBloomberg/leptonica) library for image processing.
The supported filetypes can be found in the
projects [documentation](http://www.leptonica.org/source/README.html) (Image I/O section).
"""

log = logging.getLogger(__name__)


def can_process(blob):
    """Checks if the blob can be processed by the tesseract OCR"""
    return blob.mime_type in TESSERACT_OCR_IMAGE_MIME_TYPES.union({'application/pdf'})


def create_ocr_source(name):
    """Create OcrSource object and launch Task to explore it."""
    ocr_source, created = models.OcrSource.objects.get_or_create(name=name)
    if created:
        log.info(f'OCR source "{name}" has been created')
    else:
        log.info(f'OCR source "{name}" already exists')

    walk_source.laterz(ocr_source.pk)
    log.info('ocr.walk_source task dispatched')
    return ocr_source


def dispatch_ocr_tasks():
    """Launch tasks to explore all OcrSources."""

    for ocr_source in models.OcrSource.objects.all():
        walk_source.laterz(ocr_source.pk)


def ocr_documents_for_blob(original):
    """Returns all ocrdocument objects for given md5."""

    return models.OcrDocument.objects.filter(original_hash=original.md5)


def ocr_texts_for_blob(original):
    """Yields a (source name, text) tuple for each OcrDocument matching argument."""

    for ocr_document in ocr_documents_for_blob(original):
        with ocr_document.text.open(encoding='utf8') as f:
            text = f.read()
        yield (ocr_document.source.name, text)


@snoop_task('ocr.walk_source')
def walk_source(ocr_source_pk, dir_path=''):
    """Task that explores OcrSource root directory.

    Calls [snoop.data.ocr.walk_file][] on all files found inside.

    Schedules itself recursively for all directories found on the first level, to make it work on multiple
    workers concurrently.
    """

    ocr_source = models.OcrSource.objects.get(pk=ocr_source_pk)
    for item in (ocr_source.root / dir_path).iterdir():
        if not all(ch in string.printable for ch in item.name):
            log.warn("Skipping non-printable filename %r in %s:%s",
                     item.name, ocr_source_pk, dir_path)
            continue

        if item.is_dir():
            walk_source.laterz(ocr_source.pk, f'{dir_path}{item.name}/')

        else:
            walk_file.laterz(ocr_source.pk, f'{dir_path}{item.name}')


@snoop_task('ocr.walk_file')
def walk_file(ocr_source_pk, file_path, **depends_on):
    """Task to ingest one single file found in the OcrSource directory by [snoop.data.ocr.walk_source][].

    Expects the file to have a filename ending with the MD5 and an extension that is either `.txt` or
    something else (like `.pdf`). If it's something else than `.txt`, it will run one
    [snoop.data.analyzers.tika.rmeta][] Task to get its UTF-8 text.
    """

    ocr_source = models.OcrSource.objects.get(pk=ocr_source_pk)
    path = ocr_source.root / file_path

    original_hash = path.name[:32].lower()
    assert re.match(r'^[0-9a-f]{32}$', original_hash)

    ocr_blob = models.Blob.create_from_file(path)

    if path.suffix == '.txt':
        text_blob = ocr_blob

    else:
        rmeta_blob = require_dependency(
            'tika', depends_on,
            lambda: tika.rmeta.laterz(ocr_blob),
        )
        with rmeta_blob.open(encoding='utf8') as f:
            rmeta_data = json.load(f)
        text = rmeta_data[0].get('X-TIKA:content', "")
        text_blob = models.Blob.create_from_bytes(text.encode('utf8'))

    ocr_source.ocrdocument_set.get_or_create(
        original_hash=original_hash,
        defaults={
            'ocr': ocr_blob,
            'text': text_blob,
        },
    )

    for blob in models.Blob.objects.filter(md5=original_hash):
        retry_tasks(models.Task.objects.filter(
            func='digests.gather',
            blob_arg=blob,
        ))


def run_tesseract_on_image(image_blob, lang):
    """Run a `tesseract` process on image and return result from `stdout` as blob."""

    args = [
        'tesseract',
        '--oem', '1',
        '--psm', '1',
        '-l', lang,
        str(image_blob.path()),
        'stdout'
    ]
    try:
        data = subprocess.check_output(args)
    except subprocess.CalledProcessError as e:
        if e.output:
            output = e.output.decode('latin-1')
        else:
            output = "(no output)"
        raise SnoopTaskBroken('running tesseract failed: ' + output,
                              'image_ocr_tesseract_failed')
    else:
        with models.Blob.create() as output:
            output.write(data)
        return output.blob


def run_tesseract_on_pdf(pdf_blob, lang):
    """Run a `pdf2pdfocr.py` process on PDF document and return resulting PDF as blob."""

    pdfstrlen = len(
        subprocess.check_output(f'pdftotext -q -enc UTF-8 "{pdf_blob.path()}" - | wc -w',
                                shell=True)
    )
    if pdfstrlen > settings.PDF2PDFOCR_MAX_STRLEN:
        raise SnoopTaskBroken(f'Refusing to run PDF OCR on a PDF file with {pdfstrlen} bytes of text',
                              'pdf_ocr_text_too_long')

    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp_f:
        tmp = tmp_f.name
    try:
        args = [
            'pdf2pdfocr.py',
            '-i', str(pdf_blob.path()),
            '-o', tmp,
            '-l', lang,
            '-v', '-a',
            '-x', '--oem 1 --psm 1',
            '-j', "%0.4f" % (1.0 / max(1, multiprocessing.cpu_count())),
        ]
        subprocess.check_call(args)
        return models.Blob.create_from_file(tmp)
    except subprocess.CalledProcessError as e:
        # This may as well be a non-permanent error, but we have no way to tell
        if e.output:
            output = e.output.decode('latin-1')
        else:
            output = "(no output)"
        raise SnoopTaskBroken('running pdf2pdfocr.py failed: ' + output,
                              'pdf_ocr_pdf2pdfocr_failed')
    except Exception as e:
        log.exception(e)
        raise e
    finally:
        os.remove(tmp)


@snoop_task('ocr.run_tesseract')
def run_tesseract(blob, lang):
    """Task to run Tesseract OCR on a given document.

    If it's an image, we run `tesseract` directly to extract the text. If it's a PDF, we use the
    `pdf2pdfocr.py` script to build another PDF with OCR text rendered on top of it, to make the text
    selectable.
    """
    if blob.mime_type in TESSERACT_OCR_IMAGE_MIME_TYPES:
        return run_tesseract_on_image(blob, lang)
    elif blob.mime_type == 'application/pdf':
        return run_tesseract_on_pdf(blob, lang)
