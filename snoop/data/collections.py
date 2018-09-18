# from collections import defaultdict
import logging

from django.db import transaction

from . import filesystem
from . import indexing
from . import models
# from .exportimport import build_export_queries

log = logging.getLogger(__name__)


def create_collection(name, root):
    with transaction.atomic():
        collection = models.Collection.objects.create(name=name, root=root)
        indexing.create_index(name)
        root = collection.directory_set.create()
        filesystem.walk.laterz(root.pk)

    return collection


@transaction.atomic
def delete_collection(name):

#    No need to delete the collection postgres as the database is removed
#     counts = defaultdict(int)
#
#     def delete(thing):
#         rv = thing.delete()
#         for name, count in rv[1].items():
#             counts[name] += count
#
#     collection = models.Collection.objects.get(name=name)
#     export_queries = build_export_queries(collection)
#     delete(export_queries['digests_gather_tasks'])
#     delete(export_queries['digests_index_tasks'])
#     delete(export_queries['digests_launch_tasks'])
#     delete(export_queries['emlx_reconstruct_tasks'])
#     delete(export_queries['filesystem_create_archive_files_tasks'])
#     delete(export_queries['filesystem_create_attachment_files_tasks'])
#     delete(export_queries['filesystem_handle_file_tasks'])
#     delete(export_queries['filesystem_walk_tasks'])
#     delete(collection)
#     log.info("Deleted %r: %r", collection, dict(counts))
    indexing.delete_index(name)
