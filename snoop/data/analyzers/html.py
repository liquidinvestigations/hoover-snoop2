"""Tasks that sanitize HTML files before sending to front-end.
"""

import bleach

HTML_MIME_TYPES = {'text/html', 'text/xml', 'application/xhtml+xml', 'application/xml'}


def is_html(blob):
    return blob.mime_type in HTML_MIME_TYPES


def clean(blob):
    ALLOWED_TAGS = list(bleach.ALLOWED_TAGS)
    ALLOWED_TAGS.remove('a')

    with blob.open() as f:
        html = f.read().decode('utf-8')

    return bleach.clean(html, strip=True, tags=ALLOWED_TAGS)
