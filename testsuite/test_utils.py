import pytest
from snoop.data import utils


class StingyFile:

    def __init__(self, content, chunk_size):
        self.content = content
        self.chunk_size = chunk_size

    def read(self, request_size):
        size = min([self.chunk_size, request_size])
        rv = self.content[:size]
        self.content = self.content[size:]
        return rv


CONTENT = b'some random content that will get chopped up and put back together'


@pytest.mark.parametrize('content, chunk_size, request_size', [
    (CONTENT, 65536, 1),
    (CONTENT, 65536, 10),
    (CONTENT, 65536, 65536),
    (CONTENT, 1, 100),
    (CONTENT, 2, 100),
    (CONTENT, 5, 100),
    (CONTENT, len(CONTENT), 100),
    (CONTENT, 10 * len(CONTENT), 100),
])
def test_read_minimum(content, chunk_size, request_size):
    rv = utils.read_exactly(StingyFile(content, chunk_size), request_size)
    assert rv == content[:request_size]
