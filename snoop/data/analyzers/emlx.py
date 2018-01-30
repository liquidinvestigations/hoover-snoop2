import re
import email
from .. import models
from ..tasks import shaorma, require_dependency, ShaormaBroken
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

            try:
                require_dependency(
                    f'walk_file-{parent.pk}-emlxpart-{ref}', depends_on,
                    lambda: filesystem.walk_file.laterz(parent.pk, part_name),
                )

            except ShaormaBroken as e:
                if e.reason == 'file_missing':
                    continue

                raise

            part_file = parent.child_file_set.get(name=part_name)

            with part_file.original.open() as f:
                payload = f.read()
            part.set_payload(payload)

    with models.Blob.create() as output:
        output.write(message.as_bytes())

    return output.blob
