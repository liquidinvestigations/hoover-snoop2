"""Django template tag for rendering multiple of bytes.

Used in the Admin page.
"""
# https://gist.github.com/wreckah/7307294

from math import ceil

from django import template
from django.utils.translation import gettext_lazy
from django.conf import settings

register = template.Library()

_SIZES = [
    gettext_lazy('B'),
    gettext_lazy('KB'),
    gettext_lazy('MB'),
    gettext_lazy('GB'),
    gettext_lazy('TB'),
]
_SIZES_LEN = len(_SIZES)
_DIGITS = getattr(settings, 'BYTE_SIZE_DIGITS', 3)
_LIMIT_VALUE = 10 ** _DIGITS


@register.filter
def pretty_size(size_bytes):
    """ Returns prettified size originally passed in bytes.

    >>> pretty_size(1)
    '1 B'
    >>> pretty_size(1024)
    '1 KB'
    >>> pretty_size(1024 * 1024)
    '1 MB'
    >>> pretty_size(1024 * 1024 * 1024)
    '1 GB'
    >>> pretty_size(1.111)
    '1.12 B'
    >>> pretty_size(11.111)
    '11.2 B'
    >>> pretty_size(111.111)
    '112 B'
    >>> pretty_size(2000)
    '1.96 KB'
    >>> pretty_size(21466238156.8)
    '20 GB'
    """
    if not size_bytes:
        return '0'
    try:
        size = float(size_bytes)
    except TypeError:
        return size_bytes

    # Find actual size.
    cnt = 0
    while size / 1024 >= 1 and cnt + 1 < _SIZES_LEN:
        size /= 1024
        cnt += 1

    # Limit number of decimal digits (by _DIGITS value).
    frac = 0
    while size * 10 < _LIMIT_VALUE:
        frac += 1
        size *= 10
    size = ceil(size) / 10 ** frac

    # Strip 0 and . from the right side of prettified number.
    _size = (('%%0.%df' % frac) % size).split('.')
    if len(_size) > 1:
        _size[1] = ('.' + _size[1]).rstrip('0.')

    return '%s %s' % (''.join(_size), _SIZES[cnt])
