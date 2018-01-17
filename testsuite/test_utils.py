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


@pytest.mark.parametrize('rv', [
    None,
    {'a': 13},
    'foo',
    object(),
])
def test_call_once(rv):
    call_count = 0

    @utils.run_once
    def func():
        nonlocal call_count
        call_count += 1
        return rv

    for _ in range(10):
        call_rv = func()
        assert call_rv is rv

    assert call_count == 1
