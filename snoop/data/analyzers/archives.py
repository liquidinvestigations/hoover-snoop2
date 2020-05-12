import subprocess
import tempfile
from pathlib import Path
from hashlib import sha1
import re
from ..tasks import shaorma, ShaormaBroken, returns_json_blob
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

MBOX_KNOWN_TYPES = {
    'application/mbox',
}

PDF_KNOWN_TYPES = {
    'application/pdf',
}

KNOWN_TYPES = (
    SEVENZIP_KNOWN_TYPES
    .union(READPST_KNOWN_TYPES)
    .union(MBOX_KNOWN_TYPES)
    .union(PDF_KNOWN_TYPES)
)


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

    except subprocess.CalledProcessError:
        raise ShaormaBroken('readpst failed', 'readpst_error')


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

    except subprocess.CalledProcessError:
        raise ShaormaBroken("7z extraction failed", '7z_error')


def unpack_mbox(mbox_path, output_dir):
    def slice(stream):
        last = b''
        while True:
            buffer = stream.read(1024 * 64)
            if not buffer:
                break
            window = last + buffer
            while True:
                m = re.search(br'\n\r?\n(From )', window)
                if not m:
                    break
                offset = m.start(1)
                yield window[:offset]
                window = window[offset:]
            last = window
        yield last

    with open(mbox_path, 'rb') as f:
        for n, message in enumerate(slice(f), 1):
            hash = sha1(str(n).encode('utf-8')).hexdigest()
            eml_path = Path(output_dir) / hash[:2] / '{}.eml'.format(hash)
            eml_path.parent.mkdir(parents=True, exist_ok=True)
            with eml_path.open('wb') as f:
                f.write(message)


def unpack_pdf(pdf_path, output_dir):
    try:
        subprocess.check_call(
            [
                'pdfimages',
                str(pdf_path),
                # '-all',  # only output common image types
                '-j', '-png',
                '-p',
                'page',
            ],
            stderr=subprocess.STDOUT,
            cwd=output_dir,
        )

        # As per pdfimages help text, use
        # fax2tiff to re-create the tiff files from .ccitt.
        # Using its own tiff convertor outputs images with reversed color.
        # fax = (Path(output_dir) / 'fax.tif')
        # for ccitt in Path(output_dir).glob('*.ccitt'):
        #     params = ccitt.with_suffix('.params')
        #     with params.open('r') as f:
        #         params_text = f.read().strip()
        #     subprocess.check_call(f'fax2tiff {str(ccitt)} {params_text}',
        #                           cwd=output_dir, shell=True)

        #     tif = ccitt.with_suffix('.tif')
        #     fax.rename(tif)
        #     ccitt.unlink()
        #     params.unlink()

    except subprocess.CalledProcessError:
        raise ShaormaBroken("pdfimages extraction failed", 'pdfimages_error')


def check_recursion(listing, blob_pk):
    for item in listing:
        if item['type'] == 'file':
            if item['blob_pk'] == blob_pk:
                raise RuntimeError(f"Recursion detected, blob_pk={blob_pk}")

        elif item['type'] == 'directory':
            check_recursion(item['children'], blob_pk)


@shaorma('archives.unarchive')
@returns_json_blob
def unarchive(blob):
    with tempfile.TemporaryDirectory() as temp_dir:
        if blob.mime_type in SEVENZIP_KNOWN_TYPES:
            call_7z(blob.path(), temp_dir)
        elif blob.mime_type in READPST_KNOWN_TYPES:
            call_readpst(blob.path(), temp_dir)
        elif blob.mime_type in MBOX_KNOWN_TYPES:
            unpack_mbox(blob.path(), temp_dir)
        elif blob.mime_type in PDF_KNOWN_TYPES:
            unpack_pdf(blob.path(), temp_dir)

        listing = sorted(
            list(archive_walk(Path(temp_dir))),
            key=lambda c: c['name'],
        )

    check_recursion(listing, blob.pk)

    return listing


def archive_walk(path):
    for thing in path.iterdir():
        if thing.is_dir():
            yield {
                'type': 'directory',
                'name': thing.name,
                'children': sorted(
                    list(archive_walk(thing)),
                    key=lambda c: c['name'],
                ),
            }

        else:
            yield {
                'type': 'file',
                'name': thing.name,
                'blob_pk': models.Blob.create_from_file(thing).pk,
            }
