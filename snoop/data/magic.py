import subprocess
import re
from urllib.request import urlopen
from contextlib import closing
from pathlib import Path

MAGIC_URL = 'https://github.com/hoover/magic-definitions/raw/master/magic.mgc'
MAGIC_FILE = Path(__file__).resolve().parent.parent.parent / 'magic.mgc'


class Magic:

    def __init__(self):
        self.mime_process = subprocess.Popen(
            ['file', '-', '--mime-type', '--mime-encoding', '-k'],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        self.magic_process = subprocess.Popen(
            ['file', '-', '-k'],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )

    def finish_mime(self):
        if self.mime_process:
            self.mime_process.stdin.close()
            output = self.mime_process.stdout.read().decode('latin1')
            m = re.match(
                r'/dev/stdin: (?P<mime_type>[^;].+); '
                r'charset=(?P<mime_encoding>\S+)',
                output,
            )
            self.mime_type, self.mime_encoding = (m.group('mime_type'), m.group('mime_encoding'))
            # file's -k option separates multiple findings with \012-
            # Keep only the first finding
            self.mime_type = self.mime_type.split(r'\012-')[0]
            assert self.mime_process.wait() == 0
            self.mime_process = None

    def finish_magic(self):
        if self.magic_process:
            self.magic_process.stdin.close()
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
