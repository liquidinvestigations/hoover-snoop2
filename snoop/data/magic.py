import os
import subprocess
import re
from pathlib import Path
from .utils import read_exactly

MAGIC_URL = 'https://github.com/liquidinvestigations/magic-definitions/raw/master/magic.mgc'
MAGIC_FILE = Path(os.getenv('MAGIC_FILE'))
assert MAGIC_FILE.exists()


class Magic:

    def __init__(self):
        self.mime_process = subprocess.Popen(
            [
                'file', '-', '--mime-type', '--mime-encoding', '-k',
                '-m', str(MAGIC_FILE),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        self.magic_process = subprocess.Popen(
            ['file', '-', '-k', '-m', str(MAGIC_FILE)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )

    def finish_mime(self):
        if self.mime_process:
            try:
                self.mime_process.stdin.close()
            except IOError:
                pass
            output = self.mime_process.stdout.read().decode('latin1')
            m = re.match(
                r'/dev/stdin: (?P<mime_type>[^;].+); '
                r'charset=(?P<mime_encoding>\S+)',
                output,
            )
            self.mime_type = m.group('mime_type')
            self.mime_encoding = m.group('mime_encoding')
            # file's -k option separates multiple findings with \012-
            # Keep only the first finding
            self.mime_type = self.mime_type.split(r'\012-')[0]

            exit_code = self.mime_process.wait()
            if exit_code != 0:
                msg = f"`file` exited with {exit_code}: {output}"
                raise RuntimeError(msg)

            self.mime_process = None

    def finish_magic(self):
        if self.magic_process:
            try:
                self.magic_process.stdin.close()
            except IOError:
                pass
            output = self.magic_process.stdout.read().decode('latin1')
            output = output.split(r'\012-')[0]
            m = re.match(
                r'/dev/stdin: (?P<magic_output>.+)',
                output,
            )
            self.magic_output = m.group('magic_output')

            exit_code = self.magic_process.wait()
            if exit_code != 0:
                msg = f"`file` exited with {exit_code}: {self.magic_output}"
                raise RuntimeError(msg)

            self.magic_process = None

    def finish(self):
        self.finish_mime()
        self.finish_magic()

    def update(self, buffer):
        if self.mime_process:
            try:
                self.mime_process.stdin.write(buffer)
            except IOError:
                self.finish_mime()

        if self.magic_process:
            try:
                self.magic_process.stdin.write(buffer)
            except IOError:
                self.finish_mime()


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
