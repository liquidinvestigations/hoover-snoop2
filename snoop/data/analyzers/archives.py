"""Tasks that unpack archives and return their structure and contents.
"""

import time
from contextlib import contextmanager
import logging
import subprocess
from pathlib import Path
from hashlib import sha1
import os
import tempfile
import re

import mimetypes

from ..tasks import snoop_task, SnoopTaskBroken, returns_json_blob
from .. import models
from .. import collections

log = logging.getLogger(__name__)

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

SEVENZIP_ACCEPTED_EXTENSIONS = {
    ".7z", ".apm", ".ar", ".a", ".deb", ".lib", ".arj", ".bz2", ".bzip2", ".tbz2", ".tbz", ".cab", ".chm",
    ".chi", ".chq", ".chw", ".hxs", ".hxi", ".hxr", ".hxq", ".hxw", ".lit", ".msi", ".msp", ".doc", ".xls",
    ".ppt", ".cpio", ".cramfs", ".dmg", ".elf", ".ext", ".ext2", ".ext3", ".ext4", ".img", ".fat", ".img",
    ".flv", ".gz", ".gzip", ".tgz", ".tpz", ".gpt", ".mbr", ".hfs", ".hfsx", ".ihex", ".iso", ".img",
    ".lzh", ".lha", ".lzma", ".lzma86", ".macho", ".mbr", ".mslz", ".mub", ".nsis", ".ntfs", ".img", ".exe",
    ".dll", ".sys", ".te", ".pmd", ".qcow", ".qcow2", ".qcow2c", ".rar", ".r00", ".rar", ".r00", ".rpm",
    ".001", ".squashfs", ".swf", ".swf", ".tar", ".ova", ".udf", ".iso", ".img", ".scap", ".uefif", ".vdi",
    ".vhd", ".vmdk", ".wim", ".swm", ".esd", ".xar", ".pkg", ".xz", ".txz", ".z", ".taz", ".zip", ".z01",
    ".zipx", ".jar", ".xpi", ".odt", ".ods", ".docx", ".xlsx", ".epub",
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


def can_unpack_with_7z(blob):
    """Check if the object can be unpacked with 7z. Will check both guessed extensions and mime type."""
    ext = mimetypes.guess_extension(blob.mime_type)
    return blob.mime_type in SEVENZIP_MIME_TYPES \
        or ext in SEVENZIP_ACCEPTED_EXTENSIONS


def is_archive(blob):
    """Checks if mime type is a known archive."""

    return blob.mime_type in ARCHIVES_MIME_TYPES or can_unpack_with_7z(blob)


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


def unpack_7z(archive_path, output_dir):
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


def unarchive_7z_fallback(blob):
    """Old method of unpacking archives: simply calling 7z on them."""

    with blob.mount_path() as blob_path:
        with collections.current().mount_blobs_root(readonly=False) as blobs_root:
            base = Path(blobs_root) / 'tmp' / 'archives'
            base.mkdir(parents=True, exist_ok=True)
            with tempfile.TemporaryDirectory(prefix=blob.pk, dir=base) as temp_dir:
                log.info('extracting %s into: %s', blob_path, temp_dir)
                unpack_7z(blob_path, temp_dir)

                listing = list(archive_walk(Path(temp_dir)))
                log.info('obtained listing with %s items.', len(listing))
                create_blobs(listing)
                log.info('created all blobs.')

    check_recursion(listing, blob.pk)
    return listing


@contextmanager
def mount_7z_archive(blob, blob_path):
    """Mount object from its given path onto a temporary directory.

    Directory is unmounted when context manager exits.

    Args:
        - blob: the archive to mount
        - blob_path: a path location with the above object already mounted to a filesystem
    """
    with tempfile.TemporaryDirectory(prefix='mount-7z-fuse-ng-') as temp_dir:
        with tempfile.TemporaryDirectory(prefix='mount-7z-symlink-') as symlink_dir:
            guess_ext = (mimetypes.guess_extension(blob.mime_type) or '')[:20]
            actual_extension = os.path.splitext(blob_path)[-1][:20]
            log.info('going to mount %s', str(blob_path))

            log.info('considering original extension: "%s", guessed extension: "%s"',
                     actual_extension, guess_ext)
            if actual_extension in SEVENZIP_ACCEPTED_EXTENSIONS:
                path = blob_path
                log.info('choosing supported extension in path "%s"', path)
            elif guess_ext in SEVENZIP_ACCEPTED_EXTENSIONS:
                symlink = Path(symlink_dir) / ('link' + guess_ext)
                symlink.symlink_to(blob_path)
                path = symlink
                log.info('choosing symlink with guessed extension, generated path: %s', path)
            else:
                log.info('no valid file extension in path; looking in File table...')
                path = None
                for file_entry in models.File.objects.filter(original_blob=blob):
                    file_entry_ext = os.path.splitext(file_entry.name)[-1][:20]
                    log.info('found extension: "%s" from file entry %s', file_entry_ext, file_entry)
                    if file_entry_ext in SEVENZIP_ACCEPTED_EXTENSIONS:
                        symlink = Path(symlink_dir) / ('link' + guess_ext)
                        symlink.symlink_to(blob_path)
                        path = symlink
                        log.info('choosing symlink with extension taken from File: %s', path)
                        break

                if path is None:
                    path = blob_path
                    log.info('found no Files; choosing BY DEFAULT original path: %s', path)

            subprocess.check_call(['fuse_7z_ng', '-o', 'ro', path, temp_dir])
            try:
                yield temp_dir
            finally:
                attempt = 0
                subprocess.run(['umount', temp_dir], check=False)
                while os.listdir(temp_dir):
                    time.sleep(0.05)
                    subprocess.run(['umount', temp_dir], check=False)
                    subprocess.run(['umount', '-l', temp_dir], check=False)
                    attempt += 1
                    if attempt > 100:
                        raise RuntimeError("Can't unmount 7z archive!!!")


def unarchive_7z_with_mount(blob):
    """Mount 7z archive with fuse-7z-ng and create structure from files inside."""

    with blob.mount_path() as blob_path:
        with mount_7z_archive(blob, blob_path) as temp_dir:
            listing = list(archive_walk(Path(temp_dir)))
            create_blobs(listing,
                         unlink=False,
                         archive_source_blob=blob,
                         archive_source_root=temp_dir)

    check_recursion(listing, blob.pk)
    return listing


def unarchive_7z(blob):
    """Attempt to unarchive using fuse mount; if that fails, just unpack whole file."""

    # for mounting, change the PWD into a temp dir, because
    # the fuse-7z mounting library sometimes enjoys a
    # large core dump on the PWD.
    x = os.getcwd()
    with tempfile.TemporaryDirectory(prefix='unarchive-7z-pwd-') as pwd:
        os.chdir(pwd)
        try:
            # Either mount, if possible, and if not, use the CLI to unpack into blobs
            try:
                return unarchive_7z_with_mount(blob)
            except Exception as e:
                log.exception(e)
                log.error('using old method (unpacking everything)...')
            return unarchive_7z_fallback(blob)
        finally:
            os.chdir(x)


@snoop_task('archives.unarchive', priority=2, version=2)
@returns_json_blob
def unarchive(blob):
    """Task to extract from an archive (or archive-looking) file its children.

    Runs on archives, email archives and any other file types that can contain another file (such as
    documents that embed images).
    """
    if can_unpack_with_7z(blob):
        return unarchive_7z(blob)

    unpack_func = None
    if blob.mime_type in READPST_MIME_TYPES:
        unpack_func = call_readpst
    elif blob.mime_type in MBOX_MIME_TYPES:
        unpack_func = unpack_mbox
    elif blob.mime_type in PDF_MIME_TYPES:
        unpack_func = unpack_pdf
    else:
        raise RuntimeError('unarchive: unknown mime type')

    with blob.mount_path() as blob_path:
        with collections.current().mount_blobs_root(readonly=False) as blobs_root:
            base = Path(blobs_root) / 'tmp' / 'archives'
            base.mkdir(parents=True, exist_ok=True)
            with tempfile.TemporaryDirectory(prefix=blob.pk, dir=base) as temp_dir:
                unpack_func(blob_path, temp_dir)
                listing = list(archive_walk(Path(temp_dir)))
                create_blobs(listing)

    check_recursion(listing, blob.pk)
    return listing


def archive_walk(path):
    """Generates simple dicts with archive listing for the archive."""

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


def create_blobs(dirlisting, unlink=True, archive_source_blob=None, archive_source_root=None):
    """Create blobs for files in archive listing created by [snoop.data.analyzers.archive_walk][].

    Args:
        - unlink: if enabled (default), will delete files just after they are saved to blob storage.
        - archive_source_blob: object to extract archive from. Used to record keeping.
        - archive_source_root: filesystem path where above object is mounted/extracted.
    """

    for entry in dirlisting:
        if entry['type'] == 'file':
            path = Path(entry['path'])
            if archive_source_blob:
                entry['blob_pk'] = models.Blob.create_from_file(
                    path,
                    archive_source_blob=archive_source_blob,
                    archive_source_key=os.path.relpath(path, start=archive_source_root),
                ).pk
            else:
                entry['blob_pk'] = models.Blob.create_from_file(path).pk
            if unlink:
                os.unlink(path)
            del entry['path']
        else:
            create_blobs(entry['children'],
                         unlink=unlink,
                         archive_source_blob=archive_source_blob,
                         archive_source_root=archive_source_root)
