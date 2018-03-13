import logging
import json
import subprocess
import tempfile
from pathlib import Path
from collections import defaultdict
import email
from .. import models
from ..tasks import shaorma, ShaormaError, ShaormaBroken, require_dependency
from ..tasks import returns_json_blob
from . import tika
from . import pgp

BYTE_ORDER_MARK = b'\xef\xbb\xbf'

log= logging.getLogger(__name__)


def iter_parts(message, numbers=[]):
    if message.is_multipart():
        for n, part in enumerate(message.get_payload(), 1):
            yield from iter_parts(part, numbers + [str(n)])
    else:
        yield '.'.join(numbers), message


def read_header(raw_header):
    return str(
        email.header.make_header(
            email.header.decode_header(
                raw_header
            )
        )
    )


def get_headers(message):
    rv = defaultdict(list)

    for key in message.keys():
        for raw_header in message.get_all(key):
            rv[key.title()].append(read_header(raw_header))

    return dict(rv)


def dump_part(message, depends_on):
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
    except:
        log.exception("Error getting email payload")
        raise ShaormaBroken("Error getting payload", "email_get_payload")

    if pgp.is_encrypted(payload_bytes):
        payload_bytes = pgp.decrypt(payload_bytes)
        rv['pgp'] = True

    if content_type == 'text/plain':
        charset = message.get_content_charset() or 'latin1'
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


@shaorma('email.parse')
@returns_json_blob
def parse(blob, **depends_on):
    with blob.open() as f:
        message_bytes = f.read()

    if message_bytes[:3] == BYTE_ORDER_MARK:
        message_bytes = message_bytes[3:]

    message = email.message_from_bytes(message_bytes)
    data = dump_part(message, depends_on)

    return data


@shaorma('email.msg_to_eml')
def msg_to_eml(blob):
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
            raise ShaormaError("msgconvert failed", e.output.decode('latin1'))

        return models.Blob.create_from_file(eml_path)


def parse_date(raw_date):
    return email.utils.parsedate_to_datetime(raw_date)
