import logging

from . import models
from .filesystem import walk
from .magic import download_magic_definitions
from .ocr import dispatch_ocr_tasks
from .tasks import dispatch_pending_tasks

logger = logging.getLogger(__name__)


def dispatch_walk_tasks():
    root = models.Directory.root()
    if not root:
        root = models.Directory.objects.create()
    walk.laterz(root.pk)


def run_dispatcher():
    download_magic_definitions()
    dispatch_pending_tasks()
    dispatch_walk_tasks()
    dispatch_ocr_tasks()
