"""Guess mime types from content and filename.

Uses the `file` executable (libmagic) to guess the mime type, even if the extension is incorrect.
In some cases, the correct mime type is only discovered when the extension is present. For example, all
".docx" and "xlsx" and similar ".***x" Microsoft Office files are actually zips with XMLs inside - so
impossible for `file` to differentiate from the content alone, without implementing decompression too.

Last, we have our own additions to this system, in order to try and differentiate between some ambiguous
cases even `find` doesn't take into account; such as the difference between a single E-mail file and a MBOX
collection.
"""

import logging
import subprocess
import re
from .utils import read_exactly

MIME_PROCESS_CMD = [
    'file',
    '--mime-type',
    '--mime-encoding',
    '-kbpL',
]
MIME_REGEX = re.compile(
    r'(?P<mime_type>[^;].+); '
    r'charset=(?P<mime_encoding>\S+)',
)
MAGIC_PROCESS_CMD = [
    'file', '-kbpL',
]
MAGIC_REGEX = re.compile(
    r'(?P<magic_output>.+)',
)

log = logging.getLogger(__name__)


def _parse_mime(output):
    """Parse `file` process output into `mime_type` and `mime_encoding` fields, with a regex.
    """
    output = output.decode('latin1')
    m = re.match(
        MIME_REGEX,
        output
    )
    if m:
        mime_type = m.group('mime_type')
        mime_type = mime_type.split(r'\012-')[0]
        mime_encoding = m.group('mime_encoding')
        return mime_type, mime_encoding
    else:
        return '', ''


def _parse_magic(output):
    """Parse `file` process output into `magic_output` field, with a regex.
    """
    output = output.decode('latin1')
    output = output.split(r'\012-')[0]
    m = re.match(
        MAGIC_REGEX,
        output,
    )
    if m:
        return m.group('magic_output')
    else:
        return ''


class Magic:
    """Wrapper for running various "file" commands over Blobs.

    Used internally when creating `snoop.data.models.Blob` instances.
    """
    @property
    def fields(self):
        return {
            'mime_type': self.mime_type,
            'mime_encoding': self.mime_encoding,
            'magic': self.magic_output,
        }

    def __init__(self, path):
        mime_output = _parse_mime(subprocess.check_output(MIME_PROCESS_CMD + [path]))
        self.mime_type, self.mime_encoding = mime_output

        magic_output = _parse_magic(subprocess.check_output(MAGIC_PROCESS_CMD + [path]))
        self.magic_output = magic_output

        # Emails are often badly detected by libmagic.
        # Sometimes, mimetype comes out null but magic has multipart boundary.
        should_check_email = (
            self.mime_type.startswith('text/')
            or self.magic_output.startswith('multipart/')
            or not self.mime_type
        )
        if should_check_email:
            if looks_like_email(path):
                if looks_like_emlx_email(path):
                    self.mime_type = 'message/x-emlx'
                elif looks_like_mbox(path):
                    self.mime_type = 'application/mbox'
                else:
                    self.mime_type = 'message/rfc822'

        if self.magic_output.startswith('Microsoft Outlook email folder') \
                or self.magic_output.startswith('Microsoft Outlook Personal'):
            self.mime_type = 'application/x-hoover-pst'

        if self.mime_type == 'application/x-ole-storage':
            self.mime_type = "application/vnd.ms-excel"


def looks_like_email(path):
    """Improvised check to detect RFC 822 emails.

    Will look for usual headers in the first 64K of the file.

    Needed because emails are sometimes detected by `libmagic` as text or something else.
    """

    HEADER_SET = {
        "Relay-Version", "Return-Path", "From", "To",
        "Received", "Message-Id", "Date", "In-Reply-To", "Subject",
    }
    HEADER_MIN_HIT_COUNT = 2
    HEADER_READ_SIZE = 1024 * 64

    with path.open('rb') as f:
        content = read_exactly(f, HEADER_READ_SIZE).decode('latin-1')

    headers_found = set([
        s.split(':')[0].strip().title()
        for s in content.splitlines()
        if ':' in s
    ])

    return len(headers_found.intersection(HEADER_SET)) >= HEADER_MIN_HIT_COUNT


def looks_like_emlx_email(path):
    """Improvised check to detect Apple format emails.

    Warning:
        Only looks at the first byte of the first line of the Apple-specific prefix.
        We probably want to revisit this and check the remainder of the email message too.
    """
    with path.open('rb') as f:
        content = read_exactly(f, 20).decode('latin-1')
    first_line = content.splitlines()[0]

    return first_line.strip().isdigit()


MBOX_PATTERNS = {
    r'^From ',
    r'^From: ',
    r'^Date: ',
    r'^Subject: ',
    r'^$',
}

MBOX_MINIMUM_EMAILS = 3


def looks_like_mbox(path):
    """Improvised check to detect MBOX format email archives.

    This is done by counting for usual email headers in the file.

    Warning:
        this does not detect MBOX files with less than 3 emails inside it.
    """
    emails = 0
    pending = set(MBOX_PATTERNS)

    with path.open('r', encoding='latin1') as f:
        for line in f:
            for pattern in pending:
                if re.match(pattern, line):
                    pending.remove(pattern)
                    break

            if not pending:
                pending = set(MBOX_PATTERNS)
                emails += 1

                if emails >= MBOX_MINIMUM_EMAILS:
                    return True

    return False
