from .. import models
from ..tasks import snoop_task


@snoop_task('text.extract')
def extract_text(blob):
    with models.Blob.create() as output:
        with blob.open() as src:
            output.write(src.read())

    return output.blob
