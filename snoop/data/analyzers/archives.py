"""Tasks that unpack archives and return their structure and contents.
"""

import subprocess
import tempfile
from pathlib import Path
from hashlib import sha1
import re
from ..tasks import snoop_task, SnoopTaskBroken, returns_json_blob
from .. import models
import os
import pprint
from timeit import default_timer as timer
import sys 

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
    with tempfile.TemporaryDirectory() as temp_dir:
        if blob.mime_type in SEVENZIP_MIME_TYPES:
            call_7z(blob.path(), temp_dir)
        elif blob.mime_type in READPST_MIME_TYPES:
            call_readpst(blob.path(), temp_dir)
        elif blob.mime_type in MBOX_MIME_TYPES:
            unpack_mbox(blob.path(), temp_dir)
        elif blob.mime_type in PDF_MIME_TYPES:
            unpack_pdf(blob.path(), temp_dir)

        old_listing = list(old_archive_walk(Path(temp_dir)))
        listing = list(archive_walk(Path(temp_dir)))
        create_blobs(listing)
        print('Listing:------------------')
        print(listing)
        print('Old Listing:------------------')
        print(old_listing)
        assert listing == old_listing


    print('Size:')
    print(sys.getsizeof(listing)/20**2)

    check_recursion(listing, blob.pk)

    return listing


# def archive_walk(path):
#     """Generates simple dicts with archive listing for the archive. """
#     print(path)
#     walk_iter = os.walk(path, topdown=True)
#     print(list(next(walk_iter)))
#     res = []
#     (root, dirs, files) = next(walk_iter)
#     children = []
#     for f in files:
#         file_info = {
#             'type': 'file',
#             'name': f,
#             'blob_pk': models.Blob.create_from_file(Path(os.path.join(root, f))).pk,
#         }
#         children.append(file_info)
#     root_info = {
#         'type': 'directory',
#         'parent': '',
#         'name': os.path.basename(root),
#         'children': children
#     }
#     res.append(root_info)
#     for root, dirs, files in walk_iter:
#         print("Root:")
#         print(root)
#         print("Root2:")
#         print(root)
#         print("Dirs:")
#         print(dirs)
#         print("Files:")
#         print(files)
#         children = []
#         # for d in dirs:
#         #     dir_info = {
#         #         'type': 'directory',
#         #         'name': d,
#         #         'children': [],
#         #     }
#         #     children.append(dir_info)
#         for f in files:
#             file_info = {
#                 'type': 'file',
#                 'name': f,
#                 'blob_pk': models.Blob.create_from_file(Path(os.path.join(root, f))).pk,
#             }
#             children.append(file_info)
# 
#         try:
#             parent = (list(Path(root).parent.parts)[-1])
#         except IndexError:
#             parent = b''
#         root_info = {
#             'type': 'directory',
#             # 'name': root,
#             'parent': parent,
#             'name': os.path.basename(root),
#             'children': children,
#         }
#         res.append(root_info)
#     pprint.pprint(res)
#     return res

def old_archive_walk(path):
    """Generates simple dicts with archive listing for the archive. """

    for thing in path.iterdir():
        print(thing)

        print(type(thing))
        if thing.is_dir():
            yield {
                'type': 'directory',
                'name': thing.name,
                'children': list(old_archive_walk(thing)),
            }

        else:
            print(models.Blob.create_from_file(thing).pk)
            yield {
                'type': 'file',
                'name': thing.name,
                'blob_pk': models.Blob.create_from_file(thing).pk,
                'path': str(thing),
            }
 
#def archive_walk(path):
#    d = {'name': os.path.basename(path)}
#    print(d)
#    if os.path.isdir(path):
#        d['type'] = "directory"
#        d['children'] = [archive_walk(os.path.join(path,x)) for x in os.listdir\
#(path)]
#    else:
#        d['type'] = "file"
#        d['blob_pk'] = models.Blob.create_from_file(path).pk
#    return d

def archive_walk(path):
    for entry in os.scandir(path):
        print(entry.path)
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
                # 'blob_pk': models.Blob.create_from_file(entry.path).pk,
                'path': entry.path
            }

def create_blobs(dirlisting):
    for entry in dirlisting:
        if entry['type'] == 'file':
            print(entry['path'])
            path = Path(entry['path'])
            entry['blob_pk'] = models.Blob.create_from_file(path).pk
        else:
            create_blobs(entry['children'])
    
