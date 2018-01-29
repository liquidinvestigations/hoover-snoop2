import re
import email
from .. import models
from ..tasks import shaorma, require_dependency
from .email import iter_parts


@shaorma('emlx.reconstruct')
def reconstruct(file_pk, **depends_on):
    from .. import filesystem

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

            require_dependency(
                f'walk-{parent.pk}', depends_on,
                lambda: filesystem.walk.laterz(parent.pk),
            )

            require_dependency(
                f'walk_file-{parent.pk}-part-{ext}', depends_on,
                lambda: filesystem.walk_file.laterz(parent.pk, part_name),
            )

            try:
                part_file = parent.child_file_set.get(name=part_name)

            except models.File.DoesNotExist:
                # skip this part, it's missing
                continue

            with part_file.original.open() as f:
                payload = f.read()
            part.set_payload(payload)

    with models.Blob.create() as output:
        output.write(message.as_bytes())

    return output.blob
