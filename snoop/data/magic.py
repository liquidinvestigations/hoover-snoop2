import subprocess
import re
from urllib.request import urlopen
from contextlib import closing
from pathlib import Path
from .utils import read_exactly

MAGIC_URL = 'https://github.com/hoover/magic-definitions/raw/master/magic.mgc'
MAGIC_FILE = Path(__file__).resolve().parent.parent.parent / 'magic.mgc'


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
            assert self.mime_process.wait() == 0
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

            assert self.magic_process.wait() == 0
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


def download_magic_definitions():
    from .models import chunks
    if not MAGIC_FILE.exists():
        print("Downloading magic.mgc ...")
        with MAGIC_FILE.open('wb') as f:
            with closing(urlopen(MAGIC_URL)) as resp:
                for chunk in chunks(resp):
                    f.write(chunk)
        print("ok")

    which_file = subprocess.check_output(['which', 'file']).decode('latin1')
    version_file = subprocess.check_output([
        'file', '--version',
         '-m', str(MAGIC_FILE),
    ]).decode('latin1')
    print(f"Using {which_file} with version info: \n{version_file}")


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
