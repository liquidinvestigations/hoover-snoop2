from django.db import transaction
from . import models
from . import filesystem
from . import indexing


def create_collection(name, root):
    with transaction.atomic():
        collection = models.Collection.objects.create(name=name, root=root)
        indexing.create_index(name)
        root = collection.directory_set.create()
        filesystem.walk.laterz(root.pk)

    return collection
