from contextlib import contextmanager
from pathlib import Path
import tempfile


class BlobWriter:

    def __init__(self, file):
        self.file = file

    def write(self, data):
        self.file.write(data)

    def set_filename(self, filename):
        self.filename = filename


class FlatBlobStorage:

    def __init__(self, root):
        self.root = Path(root)
        self.tmp = self.root / 'tmp'

    @contextmanager
    def save(self):
        self.root.mkdir(exist_ok=True)
        self.tmp.mkdir(exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=self.tmp, delete=False) as f:
            writer = BlobWriter(f)
            yield writer
        Path(f.name).rename(self.root / writer.filename)

    def open(self, blob_id):
        return self.path(blob_id).open('rb')

    def path(self, blob_id):
        return self.root / blob_id
