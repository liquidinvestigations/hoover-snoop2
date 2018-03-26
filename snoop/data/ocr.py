import re
import json
from pathlib import Path
from . import models
from .tasks import shaorma, require_dependency, retry_tasks
from .analyzers import tika


def create_ocr_source(name, root):
    ocr_source = models.OcrSource.objects.create(name=name, root=root)
    walk_source.laterz(ocr_source.pk)
    return ocr_source


def dispatch_ocr_tasks():
    for ocr_source in models.OcrSource.objects.all():
        walk_source.laterz(ocr_source.pk)


def ocr_documents_for_blob(original):
    return models.OcrDocument.objects.filter(original_hash=original.md5)


def ocr_texts_for_blob(original):
    for ocr_document in ocr_documents_for_blob(original):
        with ocr_document.text.open(encoding='utf8') as f:
            text = f.read()
        yield (ocr_document.source.name, text)


@shaorma('ocr.walk_source')
def walk_source(ocr_source_pk, dir_path=''):
    ocr_source = models.OcrSource.objects.get(pk=ocr_source_pk)
    for item in (Path(ocr_source.root) / dir_path).iterdir():
        if item.is_dir():
            walk_source.laterz(ocr_source.pk, f'{dir_path}{item.name}/')

        else:
            walk_file.laterz(ocr_source.pk, f'{dir_path}{item.name}')


@shaorma('ocr.walk_file')
def walk_file(ocr_source_pk, file_path, **depends_on):
    ocr_source = models.OcrSource.objects.get(pk=ocr_source_pk)
    path = Path(ocr_source.root) / file_path

    filebasename = path.name.split('.')[0]
    joined_path = ''.join(path.parent.parts + (filebasename,))
    original_hash = joined_path[-32:].lower()
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
