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
        node = node.parent_directory
    for name in reversed(path_elements):
        path /= name

    print('path:', path)
    for thing in path.iterdir():
        print(thing)
        if thing.is_dir():
            print('dir')
            (child_directory, _) = directory.child_directory_set.get_or_create(collection=directory.collection, name=thing.name)
            walk.delay(child_directory.pk)
        else:
            print('file')
