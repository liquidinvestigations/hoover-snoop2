import logging
from pathlib import Path
from . import celery
from . import models

logger = logging.getLogger(__name__)


@celery.app.task
def walk(directory_pk):
    directory = models.Directory.objects.get(pk=directory_pk)

    path_elements = []
    node = directory
    path = Path(directory.collection.root)
    while node.parent_directory:
        path_elements.append(node.name)
    for node in reversed(path_elements):
        path /= node.name

    print('path:', path)
    #logger.info('walking %r', collection)
