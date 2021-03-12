"""Various utilities.

Helper functions for singletons, binary file manipulation, timestamps.
"""

from datetime import datetime
from django.utils.timezone import utc


def run_once(func):
    not_run = object()
    rv = not_run

    def wrapper():
        nonlocal rv
        if rv is not_run:
            rv = func()
        return rv

    return wrapper


def read_exactly(f, size):
    buffer = b''
    while len(buffer) < size:
        chunk = f.read(size - len(buffer))
        if not chunk:
            break
        buffer += chunk
    return buffer


def time_from_unix(t):
    return utc.fromutc(datetime.utcfromtimestamp(t))


def zulu(t):
    if not t:
        return None
    txt = t.astimezone(utc).isoformat()
    assert txt.endswith('+00:00')
    return txt.replace('+00:00', 'Z')


def parse_zulu(txt):
    return utc.fromutc(datetime.strptime(txt, "%Y-%m-%dT%H:%M:%S.%fZ"))
