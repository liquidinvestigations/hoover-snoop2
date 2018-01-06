from time import sleep
import logging
from . import models
from . import tasks

logger = logging.getLogger(__name__)


def run_dispatcher():
    for collection in models.Collection.objects.all():
        [root] = collection.directory_set.filter(parent_directory__isnull=True).all()
        tasks.walk.delay(root.pk)
