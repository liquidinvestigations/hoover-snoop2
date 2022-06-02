"""Tasks that unpack archives and return their structure and contents.

Tables are also implemented as archives, with each row being unpacked into a text file.
"""

import csv
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
import pyexcel
from django.conf import settings

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

# see https://github.com/pyexcel/pyexcel/blob/275c4dabc491b9fd401139b83aaa79538b12bcfc/pyexcel/plugins/sources/http.py#L17  # noqa
TABLE_MIME_TYPE_OPERATOR_TABLE = {
    "application/vnd.ms-excel": "xls",
    "application/vnd.ms-excel.addin.macroEnabled.12": "xls",
    "application/vnd.ms-excel.sheet.binary.macroEnabled.12": "xls",
    "application/vnd.ms-excel.sheet.macroenabled.12": "xlsm",
    "application/vnd.ms-excel.template.macroEnabled.12": "xlsm",
    "application/vnd.oasis.opendocument.spreadsheet": "ods",
    "application/vnd.oasis.opendocument.spreadsheet-template": "ods",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.template": "xlsx",
    "application/csv": "csv",
    "application/tab-separated-values": "tsv",
    "text/csv": "csv",
    "text/html": "html",
    "text/tab-separated-values": "tsv",
}

TABLE_MIME_TYPES = set(TABLE_MIME_TYPE_OPERATOR_TABLE.keys())
CSV_DELIMITER_LIST = [':', ',', '|', '\t', ';']

ARCHIVES_MIME_TYPES = (
    SEVENZIP_MIME_TYPES
    .union(READPST_MIME_TYPES)
    .union(MBOX_MIME_TYPES)
    .union(PDF_MIME_TYPES)
    .union(TABLE_MIME_TYPES)
)


def can_unpack_with_7z(blob):
    """Check if the object can be unpacked with 7z. Will check both guessed extensions and mime type."""
    ext = mimetypes.guess_extension(blob.mime_type)
    return blob.mime_type in SEVENZIP_MIME_TYPES \
        or ext in SEVENZIP_ACCEPTED_EXTENSIONS


def guess_csv_settings(file_stream, mime_encoding):
    """Returns the csv.Dialect object if file contains start of CSV, or None otherwise."""

    GUESS_READ_LEN = 8192
    text = file_stream.read(GUESS_READ_LEN)
    if isinstance(text, bytes):
        text = text.decode(mime_encoding or 'ascii', errors='replace')
    try:
        return csv.Sniffer().sniff(text, CSV_DELIMITER_LIST)
    except csv.Error:
        return None


def is_table(blob):
    """Check if blob is table type.

    LibMagic can't detect CSV and TSV, so we use the python sniff module to check, and overwrite mime type
    if we found a match.
    """
    if blob.mime_type == 'text/plain':
        with blob.open() as f:
            dialect = guess_csv_settings(f, blob.mime_encoding)
        if not dialect:
            return False
        csv_delim = dialect.delimiter
        if csv_delim == '\t':
            blob.mime_type = 'text/tab-separated-values'
        else:
            blob.mime_type = 'text/csv'
        blob.save()
        return True
    return blob.mime_type in TABLE_MIME_TYPES


def is_archive(blob):
    """Checks if mime type is a known archive or table type.
    """

    return (
        is_table(blob)
        or (blob.mime_type in ARCHIVES_MIME_TYPES)
        or can_unpack_with_7z(blob)
    )


def call_readpst(pst_path, output_dir, **kw):
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


def _do_explode_row(row_id, row, output_path, sheet_name=None, colnames=None, mime_encoding=None):
    """Write a text file for given row.

    Text file format is `<column name> = <value>` because
    the character "=" cannot be detected as a delimiter,
    so the files we output don't get detected as CSV,
    creating a never-ending cycle.
    """
    OUT_SEPARATOR = '='
    assert OUT_SEPARATOR not in CSV_DELIMITER_LIST
    # render the whole text in memory, truncate to 200K
    MAX_CELL_LEN = 1024
    MAX_ROW_LEN = 200
    if len(row) > MAX_ROW_LEN:
        row = row[:MAX_ROW_LEN]
    if colnames and len(colnames) > MAX_ROW_LEN:
        colnames = colnames[:MAX_ROW_LEN]

    if not colnames:
        colnames = [f'C{i}' for i in range(1, 1 + len(row))]
    assert len(colnames) == len(row)
    out_filepath = output_path / (str(row_id) + '.txt')
    out_lines = []
    for v, k in zip(row, colnames):
        if len(v) > MAX_CELL_LEN:
            v = v[:MAX_CELL_LEN]
        out_lines.append(f'{k} {OUT_SEPARATOR} {v}\n')
    with open(out_filepath, 'w', encoding=mime_encoding) as f:
        f.write("".join(out_lines))


def _get_row_count(rows):
    count = 0
    for _ in rows:
        count += 1
    return count


def get_table_info(table_path, mime_type, mime_encoding):
    """Returns a dict with table sheets, column names, row and column counts, pyexcel type, extra arguments.
    """

    pyexcel_filetype = TABLE_MIME_TYPE_OPERATOR_TABLE[mime_type]
    TEXT_FILETYPES = ['csv', 'tsv', 'html']
    dialect = None
    extra_kw = dict()

    rv = {
        'sheets': [],
        'sheet-columns': {},
        'sheet-row-count': {},
        'sheet-col-count': {},
        'extra-kw': {},
        'text-mode': False,
        'pyexcel-filetype': pyexcel_filetype,
    }

    try:
        if pyexcel_filetype in TEXT_FILETYPES:
            f1 = open(table_path, 'rt', encoding=mime_encoding)
            f2 = open(table_path, 'rt', encoding=mime_encoding)
            rv['text-mode'] = True

            if pyexcel_filetype in ['csv', 'tsv']:
                dialect = guess_csv_settings(f2, mime_encoding)
                f2.seek(0)
                if dialect:
                    extra_kw['delimiter'] = dialect.delimiter
                    extra_kw['lineterminator'] = dialect.lineterminator
                    extra_kw['escapechar'] = dialect.escapechar
                    extra_kw['quotechar'] = dialect.quotechar
                    extra_kw['quoting'] = dialect.quoting
                    extra_kw['skipinitialspace'] = dialect.skipinitialspace

                    extra_kw['encoding'] = mime_encoding
                    rv['extra-kw'] = extra_kw
        else:
            f1 = open(table_path, 'rb')
            f2 = open(table_path, 'rb')
        sheets = list(
            pyexcel.iget_book(
                file_stream=f1,
                file_type=pyexcel_filetype,
                auto_detect_float=False,
                auto_detect_int=False,
                auto_detect_datetime=False,
                skip_hidden_sheets=False,
            )
        )
        for sheet in sheets:
            # get rows and count them
            rows = pyexcel.iget_array(
                file_stream=f2,
                file_type=pyexcel_filetype,
                sheet_name=sheet.name,
                auto_detect_float=False,
                auto_detect_int=False,
                auto_detect_datetime=False,
                **extra_kw,
            )
            row_count = _get_row_count(rows)
            f2.seek(0)

            rows = pyexcel.iget_array(
                file_stream=f2,
                file_type=pyexcel_filetype,
                sheet_name=sheet.name,
                auto_detect_float=False,
                auto_detect_int=False,
                auto_detect_datetime=False,
                **extra_kw,
            )
            row = list(next(rows))
            col_count = len(row)
            colnames = sheet.colnames or collections.current().default_table_head_by_len.get(col_count) or []
            rv['sheets'].append(sheet.name)
            rv['sheet-columns'][sheet.name] = colnames
            rv['sheet-row-count'][sheet.name] = row_count
            rv['sheet-col-count'][sheet.name] = col_count
    finally:
        f1.close()
        f2.close()
        pyexcel.free_resources()

    return rv


def unpack_table(table_path, output_path, mime_type=None, mime_encoding=None, **kw):
    """Unpack table (csv, excel, etc.) into text files, one for each row."""

    output_path = Path(output_path)
    assert mime_type is not None
    pyexcel_filetype = TABLE_MIME_TYPE_OPERATOR_TABLE[mime_type]
    TEXT_FILETYPES = ['csv', 'tsv', 'html']
    dialect = None
    extra_kw = dict()
    f1 = None
    f2 = None
    try:
        if pyexcel_filetype in TEXT_FILETYPES:
            f1 = open(table_path, 'rt', encoding=mime_encoding)
            f2 = open(table_path, 'rt', encoding=mime_encoding)

            if pyexcel_filetype in ['csv', 'tsv']:
                dialect = guess_csv_settings(f2, mime_encoding)
                f2.seek(0)
                if dialect:
                    extra_kw['delimiter'] = dialect.delimiter
                    extra_kw['lineterminator'] = dialect.lineterminator
                    extra_kw['escapechar'] = dialect.escapechar
                    extra_kw['quotechar'] = dialect.quotechar
                    extra_kw['quoting'] = dialect.quoting
                    extra_kw['skipinitialspace'] = dialect.skipinitialspace

                    extra_kw['encoding'] = mime_encoding
        else:
            f1 = open(table_path, 'rb')
            f2 = open(table_path, 'rb')
        sheets = list(
            pyexcel.iget_book(
                file_stream=f1,
                file_type=pyexcel_filetype,
                auto_detect_float=False,
                auto_detect_int=False,
                auto_detect_datetime=False,
                skip_hidden_sheets=False,
            )
        )
        for sheet in sheets:
            if sheet.name:
                sheet_output_path = output_path / sheet.name
            else:
                sheet_output_path = output_path
            # get rows and count them
            rows = pyexcel.iget_array(
                file_stream=f2,
                file_type=pyexcel_filetype,
                sheet_name=sheet.name,
                auto_detect_float=False,
                auto_detect_int=False,
                auto_detect_datetime=False,
                **extra_kw,
            )
            row_count = _get_row_count(rows)
            f2.seek(0)

            # split large tables, so our in-memory archive crawler doesn't crash.
            # only do the split for sizes bigger than 1.5X the limit, so we avoid
            # splitting relatively small tables.
            # Or do it if the table type isn't CSV or TSV.
            if row_count > int(1.5 * settings.TABLES_SPLIT_FILE_ROW_COUNT) \
                    or pyexcel_filetype not in TEXT_FILETYPES:
                log.info('splitting sheet "%s" with %s rows into pieces...', sheet.name, row_count)
                os.makedirs(str(sheet_output_path), exist_ok=True)

                for i in range(0, row_count, settings.TABLES_SPLIT_FILE_ROW_COUNT):
                    start_row = i
                    row_limit = settings.TABLES_SPLIT_FILE_ROW_COUNT
                    end_row = min(start_row + row_limit, row_count)
                    split_file_path = str(sheet_output_path / f'split-rows-{start_row}-{end_row}.csv')
                    log.info('writing file %s', split_file_path)
                    dest_mime_encoding = mime_encoding
                    if not dest_mime_encoding or dest_mime_encoding == 'binary':
                        dest_mime_encoding = 'utf-8'
                    pyexcel.isave_as(
                        file_stream=f2,
                        file_type=pyexcel_filetype,
                        sheet_name=sheet.name,
                        auto_detect_float=False,
                        auto_detect_int=False,
                        auto_detect_datetime=False,
                        start_row=start_row,
                        row_limit=row_limit,
                        dest_file_name=split_file_path,
                        dest_delimiter=':',
                        dest_encoding=dest_mime_encoding,
                        **extra_kw,
                    )
                    f2.seek(0)
                return

            # get row iterator again, now to read rows and explode them
            if collections.current().explode_table_rows:
                log.info('exploding rows from table...')
                os.makedirs(str(sheet_output_path), exist_ok=True)

                rows = pyexcel.iget_array(
                    file_stream=f2,
                    file_type=pyexcel_filetype,
                    sheet_name=sheet.name,
                    auto_detect_float=False,
                    auto_detect_int=False,
                    auto_detect_datetime=False,
                    **extra_kw,
                )
                for i, row in enumerate(rows):
                    row = list(row)
                    colnames = sheet.colnames or \
                        collections.current().default_table_head_by_len.get(len(row))
                    _do_explode_row(
                        i, row, sheet_output_path,
                        sheet_name=sheet.name, colnames=colnames,
                        mime_encoding=mime_encoding,
                    )
    finally:
        if f1:
            f1.close()
        if f2:
            f2.close()
        pyexcel.free_resources()


def unpack_mbox(mbox_path, output_dir, **kw):
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


def unpack_pdf(pdf_path, output_dir, **kw):
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
            log.debug('going to mount %s', str(blob_path))

            log.debug('considering original extension: "%s", guessed extension: "%s"',
                      actual_extension, guess_ext)
            if actual_extension in SEVENZIP_ACCEPTED_EXTENSIONS:
                path = blob_path
                log.debug('choosing supported extension in path "%s"', path)
            elif guess_ext in SEVENZIP_ACCEPTED_EXTENSIONS:
                symlink = Path(symlink_dir) / ('link' + guess_ext)
                symlink.symlink_to(blob_path)
                path = symlink
                log.debug('choosing symlink with guessed extension, generated path: %s', path)
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
                        log.debug('choosing symlink with extension taken from File: %s', path)
                        break

                if path is None:
                    path = blob_path
                    log.debug('found no Files; choosing BY DEFAULT original path: %s', path)

            subprocess.check_call(
                ['fuse_7z_ng', '-o', 'ro', path, temp_dir],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            try:
                yield temp_dir
            finally:
                attempt = 0
                subprocess.run(
                    ['umount', temp_dir],
                    check=False,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                while os.listdir(temp_dir):
                    time.sleep(0.05)
                    subprocess.run(
                        ['umount', temp_dir],
                        check=False,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                    subprocess.run(
                        ['umount', '-l', temp_dir],
                        check=False,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
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
                log.warning('using old method (unpacking everything)...')
            return unarchive_7z_fallback(blob)
        finally:
            os.chdir(x)


@snoop_task('archives.unarchive', priority=2, version=2, queue='filesystem')
@returns_json_blob
def unarchive(blob):
    """Task to extract from an archive (or archive-looking) file its children.

    Runs on archives, email archives and any other file types that can contain another file (such as
    documents that embed images).
    """
    unpack_func = None
    if blob.mime_type in TABLE_MIME_TYPES:
        unpack_func = unpack_table
    elif blob.mime_type in READPST_MIME_TYPES:
        unpack_func = call_readpst
    elif blob.mime_type in MBOX_MIME_TYPES:
        unpack_func = unpack_mbox
    elif blob.mime_type in PDF_MIME_TYPES:
        unpack_func = unpack_pdf
    else:
        if can_unpack_with_7z(blob):
            return unarchive_7z(blob)
        else:
            raise RuntimeError('unarchive: unknown mime type')

    with blob.mount_path() as blob_path:
        with collections.current().mount_blobs_root(readonly=False) as blobs_root:
            base = Path(blobs_root) / 'tmp' / 'archives'
            base.mkdir(parents=True, exist_ok=True)
            with tempfile.TemporaryDirectory(prefix=blob.pk, dir=base) as temp_dir:
                t0 = time.time()
                log.info('starting unpack...')
                unpack_func(blob_path, temp_dir,
                            mime_type=blob.mime_type,
                            mime_encoding=blob.mime_encoding)
                log.info('unpack done in: %s seconds', time.time() - t0)
                if not os.listdir(temp_dir):
                    log.warning('extraction resulted in no files. exiting...')
                    return None

                t0 = time.time()
                log.info('starting archive listing...')
                listing = list(archive_walk(Path(temp_dir)))
                log.info('archive listing done in: %s seconds', time.time() - t0)

                t0 = time.time()
                log.info('creating archive blobs...')
                create_blobs(listing)
                log.info('create archive blobs done in: %s seconds', time.time() - t0)

    t0 = time.time()
    log.info('checking recursion archive blobs...')
    check_recursion(listing, blob.pk)
    log.info('check recursion done in: %s seconds', time.time() - t0)
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
                    archive_source_key=os.path.relpath(
                        path,
                        start=archive_source_root,
                    ).encode('utf-8', errors='surrogateescape'),
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
