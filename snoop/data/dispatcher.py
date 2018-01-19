import logging
from . import models
from .filesystem import walk
from .magic import download_magic_definitions
from .tasks import dispatch_pending_tasks

logger = logging.getLogger(__name__)


def dispatch_walk_tasks():
    for collection in models.Collection.objects.all():
        [root] = collection.directory_set.filter(
            parent_directory__isnull=True,
            container_file__isnull=True
        ).all()
        walk.laterz(root.pk)


def run_dispatcher():
    download_magic_definitions()
    dispatch_pending_tasks()
    dispatch_walk_tasks()
