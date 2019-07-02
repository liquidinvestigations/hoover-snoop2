import re
import email
import logging
from .. import models
from ..tasks import shaorma
from .email import iter_parts

log = logging.getLogger(__name__)


@shaorma('emlx.reconstruct')
def reconstruct(file_pk, **depends_on):
    from .. import filesystem  # noqa: F401

    file = models.File.objects.get(pk=file_pk)
    with file.original.open() as f:
        original_data = f.read()

    eml_data = re.sub(rb'^\d+\s+', b'', original_data, re.MULTILINE)
    message = email.message_from_bytes(eml_data)

    for ref, part in iter_parts(message):
        if part.get('X-Apple-Content-Length'):
            ext = f'.{ref}.emlxpart'
            part_name = re.sub(r'\.partial\.emlx$', ext, file.name)
            parent = file.parent_directory
            part_file = (
                parent.child_file_set
                .filter(name_bytes=part_name.encode('utf8'))
                .first()
            )

            if not part_file:
                log.warning("Missing %r", part_name)
                continue

            with part_file.original.open() as f:
                payload = f.read()
            part.set_payload(payload)

    with models.Blob.create() as output:
        output.write(message.as_bytes())

    return output.blob
