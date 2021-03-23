"""Various utilities.

Helper functions for singletons, binary file manipulation, timestamps.
"""

from datetime import datetime
from django.utils.timezone import utc


def run_once(func):
    """Decorator used to only run some function once.

    Used when said function should run in the import context.
    """
    not_run = object()
    rv = not_run

    def wrapper():
        nonlocal rv
        if rv is not_run:
            rv = func()
        return rv

    return wrapper


def read_exactly(f, size):
    """Makes repeated `f.read` calls until the buffer is of the specified size (or the file is empty).

    This is needed because `read()` will happily return less than the provided size, but we want fixed size
    chunks.
    """
    buffer = b''
    while len(buffer) < size:
        chunk = f.read(size - len(buffer))
        if not chunk:
            break
        buffer += chunk
    return buffer


def time_from_unix(t):
    """Convert UTC timestamp int to its corresponding DateTime."""
    return utc.fromutc(datetime.utcfromtimestamp(t))


def zulu(t):
    """Renders the DateTime into a timezone-aware ISO Format date."""

    if not t:
        return None
    txt = t.astimezone(utc).isoformat()
    assert txt.endswith('+00:00')
    return txt.replace('+00:00', 'Z')


def parse_zulu(txt):
    """Parses an ISO format date into a DateTime."""

    return utc.fromutc(datetime.strptime(txt, "%Y-%m-%dT%H:%M:%S.%fZ"))
