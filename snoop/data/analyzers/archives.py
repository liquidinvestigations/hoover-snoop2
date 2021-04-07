"""Tasks that unpack archives and return their structure and contents.
"""

import subprocess
from pathlib import Path
from hashlib import sha1
import re
from ..tasks import snoop_task, SnoopTaskBroken, returns_json_blob
from .. import models
from .. import collections
import os

SEVENZIP_MIME_TYPES = {
    'application/x-7z-compressed',
    'application/zip',
    'application/x-zip',
    'application/x-rar',
    'application/rar',
    'application/x-gzip',
    'application/gzip',
    'application/x-bzip2',
    'application/x-tar',
}

READPST_MIME_TYPES = {
    'application/x-hoover-pst',
}

MBOX_MIME_TYPES = {
    'application/mbox',
}

PDF_MIME_TYPES = {
    'application/pdf',
}

ARCHIVES_MIME_TYPES = (
    SEVENZIP_MIME_TYPES
    .union(READPST_MIME_TYPES)
    .union(MBOX_MIME_TYPES)
    .union(PDF_MIME_TYPES)
)


def is_archive(mime_type):
    """Checks if mime type is a known archive."""

    return mime_type in ARCHIVES_MIME_TYPES


def call_readpst(pst_path, output_dir):
    """Helper function that calls a `readpst` process."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
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
        raise SnoopTaskBroken('readpst failed', 'readpst_error')


def call_7z(archive_path, output_dir):
    """Helper function that calls a `7z` process."""

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
        raise SnoopTaskBroken("7z extraction failed", '7z_error')


def unpack_mbox(mbox_path, output_dir):
    """Split a MBOX into emails."""

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
    """Extract images from pdf by calling `pdfimages`."""

    try:
        subprocess.check_call(
            [
                'pdfimages',
                str(pdf_path),
                # '-all',  # only output common image types
                '-j', '-png', '-ccitt',
                '-p',
                'page',
            ],
            stderr=subprocess.STDOUT,
            cwd=output_dir,
        )

        # As per pdfimages help text, use
        # fax2tiff to re-create the tiff files from .ccitt, then get pngs from tiff.
        # Using its own tiff convertor outputs images with reversed color.
        fax = (Path(output_dir) / 'fax.tif')
        for ccitt in Path(output_dir).glob('*.ccitt'):
            params = ccitt.with_suffix('.params')
            png = ccitt.with_suffix('.png')

            with params.open('r') as f:
                params_text = f.read().strip()
            subprocess.check_call(f'fax2tiff {str(ccitt)} {params_text}',
                                  cwd=output_dir, shell=True)
            ccitt.unlink()
            params.unlink()

            subprocess.check_call(f'convert {str(fax)} {str(png)}',
                                  shell=True, cwd=output_dir)
            fax.unlink()

    except subprocess.CalledProcessError:
        raise SnoopTaskBroken("pdfimages extraction failed", 'pdfimages_error')


def check_recursion(listing, blob_pk):
    """Raise exception if archive (blob_pk) is contained in itself (listing)."""

    for item in listing:
        if item['type'] == 'file':
            if item['blob_pk'] == blob_pk:
                raise RuntimeError(f"Recursion detected, blob_pk={blob_pk}")

        elif item['type'] == 'directory':
            check_recursion(item['children'], blob_pk)


@snoop_task('archives.unarchive', priority=2)
@returns_json_blob
def unarchive(blob):
    """Task to extract from an archive (or archive-looking) file its children.

    Runs on archives, email archives and any other file types that can contain another file (such as
    documents that embed images).
    """
    with collections.current().tmp_dir / str(blob) as temp_dir:
        if blob.mime_type in SEVENZIP_MIME_TYPES:
            call_7z(blob.path(), temp_dir)
        elif blob.mime_type in READPST_MIME_TYPES:
            call_readpst(blob.path(), temp_dir)
        elif blob.mime_type in MBOX_MIME_TYPES:
            unpack_mbox(blob.path(), temp_dir)
        elif blob.mime_type in PDF_MIME_TYPES:
            unpack_pdf(blob.path(), temp_dir)

        listing = list(archive_walk(Path(temp_dir)))
        create_blobs(listing)

    check_recursion(listing, blob.pk)

    return listing


def archive_walk(path):
    """Generates simple dicts with archive listing for the archive. """

    for entry in os.scandir(path):
        if entry.is_dir(follow_symlinks=False):
            yield {
                'type': 'directory',
                'name': entry.name,
                'children': list(archive_walk(entry.path)),
            }
        else:
            yield {
                'type': 'file',
                'name': entry.name,
                'path': entry.path
            }


def create_blobs(dirlisting):
    """Create blobs for files in archive listing created by [snoop.data.analyzers.archive_walk."""

    for entry in dirlisting:
        if entry['type'] == 'file':
            path = Path(entry['path'])
            entry['blob_pk'] = models.Blob.create_from_file(path).pk
            del entry['path']
        else:
            create_blobs(entry['children'])
