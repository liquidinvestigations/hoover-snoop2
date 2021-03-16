import bleach

ALLOWED_TAGS = list(bleach.ALLOWED_TAGS)
ALLOWED_TAGS.remove('a')

HTML_MIME_TYPES = {'text/html', 'text/xml', 'application/xhtml+xml', 'application/xml'}


def is_html(blob):
    return blob.mime_type in HTML_MIME_TYPES


def clean(blob):
    with blob.open(encoding=blob.mime_encoding) as f:
        html = f.read()

    return bleach.clean(html, strip=True, tags=ALLOWED_TAGS)
