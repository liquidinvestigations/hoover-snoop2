"""Tasks that sanitize HTML files before sending to front-end.
"""

import bleach


def is_html(blob):
    [main_type, subtype] = blob.mime_type.split('/')

    if main_type in ['text', 'application']:
        if (subtype.startswith('html')
                or subtype.startswith('xhtml')
                or subtype.startswith('xml')):
            return True

    return False


def clean(blob):
    ALLOWED_TAGS = list(bleach.ALLOWED_TAGS)
    ALLOWED_TAGS.remove('a')

    with blob.open(encoding=blob.mime_encoding) as f:
        html = f.read()

    return bleach.clean(html, strip=True, tags=ALLOWED_TAGS)
