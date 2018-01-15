import logging
from . import models
from .filesystem import walk
from .magic import download_magic_definitions

logger = logging.getLogger(__name__)


def run_dispatcher():
    download_magic_definitions()
    for collection in models.Collection.objects.all():
        [root] = collection.directory_set.filter(
            parent_directory__isnull=True,
            container_file__isnull=True
        ).all()
        walk.laterz(root.pk)
