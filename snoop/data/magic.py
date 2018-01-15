import subprocess
import re
from urllib.request import urlopen
from contextlib import closing
from pathlib import Path

MAGIC_URL = 'https://github.com/hoover/magic-definitions/raw/master/magic.mgc'
MAGIC_FILE = Path(__file__).resolve().parent.parent.parent / 'magic.mgc'


class Magic:

    def __init__(self):
        self.process = subprocess.Popen(
            ['file', '-', '--mime-type', '--mime-encoding'],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )

    def finish(self):
        if self.process:
            self.process.stdin.close()
            output = self.process.stdout.read().decode('latin1')
            m = re.match(
                r'/dev/stdin: (?P<mime_type>[^;].+); '
                r'charset=(?P<mime_encoding>\S+)',
                output,
            )
            self.mime_type, self.mime_encoding = (m.group('mime_type'), m.group('mime_encoding'))
            assert self.process.wait() == 0
            self.process = None

    def update(self, buffer):
        if not self.process:
            return

        try:
            self.process.stdin.write(buffer)
        except IOError:
            self.finish()


def download_magic_definitions():
    from .models import chunks
    if not MAGIC_FILE.exists():
        print("Downloading magic.mgc ...")
        with MAGIC_FILE.open('wb') as f:
            with closing(urlopen(MAGIC_URL)) as resp:
                for chunk in chunks(resp):
                    f.write(chunk)
        print("ok")
