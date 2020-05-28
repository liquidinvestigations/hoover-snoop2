import tempfile
import re
import json
import logging
import string
import subprocess
import multiprocessing

from django.conf import settings
from . import models
from .tasks import snoop_task, require_dependency, retry_tasks
from .analyzers import tika


log = logging.getLogger(__name__)


def create_ocr_source(name):
    ocr_source, created = models.OcrSource.objects.get_or_create(name=name)
    if created:
        log.info(f'OCR source "{name}" has been created')
    else:
        log.info(f'OCR source "{name}" already exists')

    walk_source.laterz(ocr_source.pk)
    log.info('ocr.walk_source task dispatched')
    return ocr_source


def dispatch_ocr_tasks():
    log.info('Dispatching ocr tasks.')

    for ocr_source in models.OcrSource.objects.all():
        walk_source.laterz(ocr_source.pk)


def ocr_documents_for_blob(original):
    return models.OcrDocument.objects.filter(original_hash=original.md5)


def ocr_texts_for_blob(original):
    for ocr_document in ocr_documents_for_blob(original):
        with ocr_document.text.open(encoding='utf8') as f:
            text = f.read()
        yield (ocr_document.source.name, text)


@snoop_task('ocr.walk_source')
def walk_source(ocr_source_pk, dir_path=''):
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
    ocr_source = models.OcrSource.objects.get(pk=ocr_source_pk)
    path = ocr_source.root / file_path

    original_hash = path.name[:32].lower()
    assert re.match(r'^[0-9a-f]{32}$', original_hash)

    ocr_blob = models.Blob.create_from_file(path)

    if path.suffix == '.txt':
        text_blob = ocr_blob

    else:
        rmeta_blob = require_dependency(
            f'tika', depends_on,
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
    args = [
        'tesseract',
        '--oem', '1',
        '--psm', '1',
        '-l', lang,
        str(image_blob.path()),
        'stdout'
    ]
    data = subprocess.check_output(args)

    with models.Blob.create() as output:
        output.write(data)
    return output.blob


def run_tesseract_on_pdf(pdf_blob, lang):
    pdfstrlen = int(
        subprocess.check_output(f'pdftotext -q -env UTF-8 {pdf_blob.path()} - | wc -w',
                                shell=True).decode('utf8')
    )
    if pdfstrlen > settings.PDF2PDFOCR_MAX_WORD_COUNT:
        log.warning(f'Refusing to run PDF OCR on a PDF file with {pdfstrlen} words of text')  # noqa: E501
        return None

    with tempfile.NamedTemporaryFile() as f:
        args = [
            'pdf2pdfocr.py', '-i', pdf_blob.path(), '-o', f.name,
            '-l', lang,
            '-x', '--oem 1 --psm 1',
            '-j', "%0.4f" % (1.0 / multiprocessing.cpu_count()),
        ]
        subprocess.check_call(args)
        return models.Blob.create_from_file(f.name)


@snoop_task('ocr.run_tesseract')
def run_tesseract(blob, lang):
    if blob.mime_type.startswith('image/'):
        return run_tesseract_on_image(blob, lang)
    elif blob.mime_type == 'application/pdf':
        return run_tesseract_on_pdf(blob, lang)
