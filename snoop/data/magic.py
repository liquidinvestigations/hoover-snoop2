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


def _parse_mime(output):
    output = output.decode('latin1')
    m = re.match(
        MIME_REGEX,
        output
    )
    mime_type = m.group('mime_type')
    mime_type = mime_type.split(r'\012-')[0]
    mime_encoding = m.group('mime_encoding')
    return mime_type, mime_encoding


def _parse_magic(output):
    output = output.decode('latin1')
    output = output.split(r'\012-')[0]
    m = re.match(
        MAGIC_REGEX,
        output,
    )
    return m.group('magic_output')


class Magic:
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

        if self.mime_type.startswith('text/'):
            if looks_like_email(path):
                if looks_like_emlx_email(path):
                    self.mime_type = 'message/x-emlx'
                elif looks_like_mbox(path):
                    self.mime_type = 'application/mbox'
                else:
                    self.mime_type = 'message/rfc822'

        if self.magic_output.startswith('Microsoft Outlook email folder'):
            self.mime_type = 'application/x-hoover-pst'


def looks_like_email(path):
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
