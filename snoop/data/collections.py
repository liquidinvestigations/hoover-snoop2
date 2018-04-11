from django.db import transaction
from . import models
from . import filesystem


def create_collection(name, root):
    with transaction.atomic():
        collection = models.Collection.objects.create(name=name, root=root)
        root = collection.directory_set.create()
        filesystem.walk.laterz(root.pk)

    return collection
