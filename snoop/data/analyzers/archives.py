import json
import subprocess
import tempfile
from pathlib import Path
from ..tasks import shaorma, ShaormaError
from .. import models


SEVENZIP_KNOWN_TYPES = {
    'application/x-7z-compressed',
    'application/zip',
    'application/x-zip',
    'application/x-rar',
    'application/x-gzip',
    'application/x-bzip2',
    'application/x-tar',
}

READPST_KNOWN_TYPES = {
    'application/x-hoover-pst',
}

KNOWN_TYPES = SEVENZIP_KNOWN_TYPES.union(READPST_KNOWN_TYPES)


def is_archive(mime_type):
    return mime_type in KNOWN_TYPES


def call_readpst(pst_path, output_dir):
    try:
        subprocess.check_output([
            'readpst',
            '-D',
            '-M',
            '-e',
            '-o',
            str(output_dir),
            '-teajc',
            str(pst_path),
        ], stderr=subprocess.STDOUT)

    except subprocess.CalledProcessError as e:
        raise ShaormaError('readpst failed: ' + e.output.decode('latin1'))


def call_7z(archive_path, output_dir):
    try:
        subprocess.check_output([
            '7z',
            '-y',
            '-pp',
            'x',
            str(archive_path),
            '-o' + str(output_dir),
        ], stderr=subprocess.STDOUT)

    except subprocess.CalledProcessError as e:
        raise ShaormaError("7z extraction failed", e.output.decode('latin1'))


@shaorma
def unarchive(blob_pk):
    with tempfile.TemporaryDirectory() as temp_dir:
        blob = models.Blob.objects.get(pk=blob_pk)
        if blob.mime_type in SEVENZIP_KNOWN_TYPES:
            call_7z(blob.path(), temp_dir)
        elif blob.mime_type in READPST_KNOWN_TYPES:
            call_readpst(blob.path(), temp_dir)

        listing = list(archive_walk(Path(temp_dir)))

    with tempfile.NamedTemporaryFile() as f:
        f.write(json.dumps(listing).encode('utf-8'))
        f.flush()
        listing_blob = models.Blob.create_from_file(Path(f.name))

    return listing_blob


def archive_walk(path):
    for thing in path.iterdir():
        if thing.is_dir():
            yield {
                'type': 'directory',
                'name': thing.name,
                'children': list(archive_walk(thing)),
            }

        else:
            yield {
                'type': 'file',
                'name': thing.name,
                'blob_pk': models.Blob.create_from_file(thing).pk,
            }
