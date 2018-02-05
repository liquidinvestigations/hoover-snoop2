import logging
from . import models
from .filesystem import walk
from .magic import download_magic_definitions
from .tasks import dispatch_pending_tasks
from .ocr import dispatch_ocr_tasks

logger = logging.getLogger(__name__)


def dispatch_walk_tasks():
    for collection in models.Collection.objects.all():
        walk.laterz(collection.root_directory.pk)


def run_dispatcher():
    download_magic_definitions()
    dispatch_pending_tasks()
    dispatch_walk_tasks()
    dispatch_ocr_tasks()
