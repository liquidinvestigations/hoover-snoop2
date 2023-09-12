"""Various utilities.

Helper functions for singletons, binary file manipulation, timestamps.
"""
import fcntl
import os
import logging
from functools import wraps
from contextlib import contextmanager

from datetime import datetime
from django.utils.timezone import utc

logger = logging.getLogger(__name__)
LOCK_FILE_BASE = '/tmp'


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


def read_exactly(f, size, text_mode=False):
    """Makes repeated `f.read` calls until the buffer is of the specified size (or the file is empty).

    This is needed because `read()` will happily return less than the provided size, but we want fixed size
    chunks.
    """

    if text_mode:
        buffer = ''
    else:
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


def _flock_acquire_nb(lock_path):
    """Acquire lock file at given path.

    Lock is exclusive, errors return immediately instead of waiting."""
    open_mode = os.O_RDWR | os.O_CREAT | os.O_TRUNC
    fd = os.open(lock_path, open_mode)
    try:
        # LOCK_EX = exclusive
        # LOCK_NB = not blocking
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except Exception as e:
        os.close(fd)
        logger.warning('failed to get lock at ' + lock_path + ": " + str(e))
        raise

    return fd


def _flock_release(fd):
    """Release lock file at given path."""
    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


@contextmanager
def _flock_contextmanager_nb(lock_path):
    """Creates context with exclusive file lock at given path."""
    fd = _flock_acquire_nb(lock_path)
    try:
        yield
    finally:
        _flock_release(fd)


def flock(func):
    """Function decorator that makes use of exclusive file lock to ensure
    only one function instance is running at a time.

    If another instance is running, this returns None immediately.

    All function runners must be present on the same container for this to work."""
    file_name = f'_snoop_flock_{func.__name__}.lock'
    lock_path = os.path.join(LOCK_FILE_BASE, file_name)

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            our_err = True
            with _flock_contextmanager_nb(lock_path):
                our_err = False
                return func(*args, **kwargs)
        except Exception as e:
            if our_err:
                logger.warning(
                    'failed to get lock (maybe already running): %s, %s',
                    func.__name__,
                    str(e),
                )
                return
            raise
    return wrapper


@contextmanager
def open_exclusive(file_path, *args, **kwargs):
    """Context manager that uses exclusive blocking flock
    to ensure singular access to opened file."""

    def lock_file(fd):
        fcntl.flock(fd, fcntl.LOCK_EX)

    def unlock_file(fd):
        fcntl.flock(fd, fcntl.LOCK_UN)

    f = open(file_path, *args, **kwargs)
    lock_file(f.fileno())
    try:
        yield f
    finally:
        f.flush()
        os.fsync(f.fileno())
        unlock_file(f.fileno())
        f.close()


def flock_blocking(func):
    """Function decorator that makes use of exclusive file lock to ensure
    only one function instance is running at a time.

    If another instance is running, this waits until that one stops.

    All function runners must be present on the same container for this to work."""

    file_name = f'_snoop_flock_{func.__name__}.lock'
    lock_path = os.path.join(LOCK_FILE_BASE, file_name)

    @wraps(func)
    def wrapper(*args, **kwargs):
        with open_exclusive(lock_path, 'a+'):
            return func(*args, **kwargs)
    return wrapper
