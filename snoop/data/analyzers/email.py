"""Tasks that handle parsing e-mail.
"""

import logging
import json
import subprocess
import tempfile
from pathlib import Path
from collections import defaultdict
import email
import codecs
import chardet
from .. import models
from ..utils import zulu
from ..tasks import snoop_task, SnoopTaskBroken, require_dependency
from ..tasks import returns_json_blob
from . import tika
import re
from . import pgp

BYTE_ORDER_MARK = b'\xef\xbb\xbf'

OUTLOOK_POSSIBLE_MIME_TYPES = [
    'application/vnd.ms-outlook',
    'application/vnd.ms-office',
    'application/CDFV2',
]

EMAIL_DOMAIN_EXP = re.compile("@([\\w.-]+)")

log = logging.getLogger(__name__)


def _extract_domain(text):
    """Extract domain from email address."""

    match = EMAIL_DOMAIN_EXP.search(text)
    if match:
        return match[1]


def lookup_other_encodings(name: str) -> codecs.CodecInfo:
    """Used to set `ucs-2le` as an alias of `utf-16-le` in the codec registry.

    Used with [codecs.regiter](https://docs.python.org/3/library/codecs.html#codecs.register)
    when importing this function.
    """

    if name == 'ucs-2le':
        return codecs.lookup('utf-16-le')


codecs.register(lookup_other_encodings)


def iter_parts(message, numbers=[]):
    """Yields multipart messages into identifiable parts.

    The numbers are the positions in each part of the tree.
    """
    if message.is_multipart():
        for n, part in enumerate(message.get_payload(), 1):
            yield from iter_parts(part, numbers + [str(n)])
    else:
        yield '.'.join(numbers), message


def read_header(raw_header):
    """Parse multi-encoding header value.

    Under RFC 822, headers can be encoded in more than one character encoding. This is needed to create
    header lines like `Subject: トピック ` when you can't express `Subject` in the Japanese encoding. (In
    this documentation both are UTF-8, but in various datasets, older Windows Cyrillic encodings have this
    problem).

    See [email.header.make_header](https://docs.python.org/3/library/email.header.html#email.header.make_header)
    and [email.header.decode_header](https://docs.python.org/3/library/email.header.html#email.header.decode_header).
    """  # noqa: E501

    try:
        return str(
            email.header.make_header(
                email.header.decode_header(
                    raw_header
                )
            )
        )
    except UnicodeDecodeError:
        return str(raw_header)


def get_headers(message):
    """Extract dict with headers from email message."""

    rv = defaultdict(list)

    for key in message.keys():
        for raw_header in message.get_all(key):
            rv[key.title()].append(read_header(raw_header))

    return dict(rv)


def dump_part(message, depends_on):
    """Recursive function to extract text and attachments from multipart email.


    For `text/html` multipart fragments we use Tika to extract the text.

    Args:
        message: the multipart message.
        depends_on: dict with dependent functions; passed from the task function here to order the Tika
            processing (for text extraction) if needed.
    """

    rv = {'headers': get_headers(message)}

    if message.is_multipart():
        rv['parts'] = [
            dump_part(part, depends_on)
            for part in message.get_payload()
        ]
        return rv

    content_type = message.get_content_type()

    try:
        payload_bytes = message.get_payload(decode=True)
    except:  # noqa: E722
        log.exception("Error getting email payload")
        raise SnoopTaskBroken("Error getting payload", "email_get_payload")

    if pgp.is_encrypted(payload_bytes):
        payload_bytes = pgp.decrypt(payload_bytes)
        rv['pgp'] = True

    if content_type == 'text/plain':
        result = chardet.detect(payload_bytes)
        charset = result.get('encoding') or 'latin1'
        try:
            rv['text'] = payload_bytes.decode(charset, errors='replace')
        except (UnicodeDecodeError, LookupError) as e:
            log.exception(e)
            charset = 'latin1'
            rv['text'] = payload_bytes.decode(charset, errors='replace')

    if content_type == 'text/html':
        with models.Blob.create() as writer:
            writer.write(payload_bytes)

        rmeta_blob = require_dependency(
            f'tika-html-{writer.blob.pk}', depends_on,
            lambda: tika.rmeta.laterz(writer.blob),
        )

        with rmeta_blob.open(encoding='utf8') as f:
            rmeta_data = json.load(f)
        rv['text'] = rmeta_data[0].get('X-TIKA:content', "")

    if message.get_content_disposition():
        raw_filename = message.get_filename()
        if raw_filename:
            filename = read_header(raw_filename)

            with models.Blob.create() as writer:
                writer.write(payload_bytes)

            rv['attachment'] = {
                'name': filename,
                'blob_pk': writer.blob.pk,
            }

    return rv


@snoop_task('email.parse', priority=3)
@returns_json_blob
def parse(blob, **depends_on):
    """Task function to parse emails into a dict with its structure."""

    with blob.open() as f:
        message_bytes = f.read()

    if message_bytes[:3] == BYTE_ORDER_MARK:
        message_bytes = message_bytes[3:]

    message = email.message_from_bytes(message_bytes)
    data = dump_part(message, depends_on)

    return data


def email_meta(email_data):
    """Returns ready-to-index fields extracted from the
    [emails.parse Task][snoop.data.analyzers.email.parse].

    The fields include important email headers combined into fields, as well as a dump of all the email
    headers.
    """

    def iter_parts(email_data):
        yield email_data
        for part in email_data.get('parts') or []:
            yield from iter_parts(part)

    if not email_data:
        return {}

    headers = email_data['headers']

    text_bits = []
    pgp = False
    for part in iter_parts(email_data):
        part_text = part.get('text')
        if part_text:
            text_bits.append(part_text)

        if part.get('pgp'):
            pgp = True

    ret = {}
    convert = {
        'to': ['To', 'Cc', 'Bcc', 'Resent-To', 'Resent-Cc', 'Resent-Bcc'],
        'to-direct': ['To', 'Resent-To'],
        'cc': ['Cc', 'Resent-Cc'],
        'bcc': ['Bcc', 'Resent-Bcc'],
        'from': ['From', 'Resent-From'],
        'message-id': ['Message-Id'],
        'in-reply-to': ['In-Reply-To', 'References', 'Original-Message-ID', 'Resent-Message-Id'],
    }

    for header in convert:
        all_values = []
        for val in headers.get(header, []):
            for line in val.strip().splitlines():
                line = line.strip()
                if line and line not in all_values:
                    all_values.append(line)
        ret[header] = all_values

    message_date = None
    message_raw_date = headers.get('Date', [None])[0]
    if message_raw_date:
        message_date = zulu(parse_date(message_raw_date))

    to_domains = [_extract_domain(to) for to in ret['to']]
    from_domains = [_extract_domain(f) for f in ret['from']]
    email_domains = list(set(to_domains + from_domains))

    ret.update({
        'email-domains': [d.lower() for d in email_domains if d],
        'subject': headers.get('Subject', [''])[0],
        'text': '\n\n'.join(text_bits).strip(),
        'pgp': pgp,
        'date': message_date,
        'email-header-key': list(set(headers.keys())),
        'email-header': sum(([k + '=' + v for v in headers[k]] for k in headers), start=[]),
    })
    # not here:
    # "thread-index": {"type": "keyword"},
    #       "message": {"type": "keyword"},

    # delete empty values for all headers
    for k in list(ret.keys()):
        if not ret[k]:
            del ret[k]
    return ret


@snoop_task('email.msg_to_eml', priority=2)
def msg_to_eml(blob):
    """Task to convert `.msg` emails into `.eml`."""

    with tempfile.TemporaryDirectory() as temp_dir:
        msg_path = Path(temp_dir) / 'email.msg'
        msg_path.symlink_to(blob.path())
        eml_path = msg_path.with_suffix('.eml')

        try:
            subprocess.check_output(
                ['msgconvert', 'email.msg'],
                cwd=temp_dir,
                stderr=subprocess.STDOUT
            )
        except subprocess.CalledProcessError as e:
            # This may as well be a non-permanent error, but we have no way to tell
            if e.output:
                output = e.output.decode('latin-1')
            else:
                output = "(no output)"
            raise SnoopTaskBroken('running msgconvert failed: ' + output,
                                  'msgconvert_failed')

        return models.Blob.create_from_file(eml_path)


def parse_date(raw_date):
    """Parse the date format inside emails, returning `None` if failed."""
    try:
        return email.utils.parsedate_to_datetime(raw_date)
    except TypeError as e:
        log.exception(f'error in parsing date: "{raw_date}"  {str(e)}')
        return None
