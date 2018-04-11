from django.db import transaction
from . import models


def create_collection(name, root):
    with transaction.atomic():
        collection = models.Collection.objects.create(name=name, root=root)
        root = collection.directory_set.create()

    return collection
