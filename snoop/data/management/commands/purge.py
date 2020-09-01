import os
import shutil

from django.core.management.base import BaseCommand
from django.conf import settings
from ...logs import logging_for_management_command

from ... import indexing
from ... import collections


def confirm():
    return input('ALL data printed above will be DELETED (type "yes" to confirm):') == 'yes'


class Command(BaseCommand):
    help = "Retry running task"

    def add_arguments(self, parser):
        parser.add_argument('--force', action='store_true',
                            help="Don't ask for confirmation")

    def handle(self, **options):
        logging_for_management_command(options['verbosity'])

        def print_items(name, items):
            if not items:
                print(f'{name} to delete: none')
            else:
                print(f'{name} to delete ({len(items)}): {", ".join(items)}')
            print()

        print(len(collections.ALL),
              'collections in "liquid.ini": ',
              ', '.join(collections.ALL.keys()))

        es_indexes = set(indexing.all_indices())
        active_indexes = set(c.es_index for c in collections.ALL.values())
        es_to_delete = es_indexes - active_indexes
        print_items('ElasticSearch indexes', es_to_delete)

        dbs = set(collections.all_collection_dbs())
        active_dbs = set(c.db_name for c in collections.ALL.values())
        db_to_delete = dbs - active_dbs
        print_items('Databases', db_to_delete)

        blobs = set(os.listdir(settings.SNOOP_BLOB_STORAGE))
        blobs_to_delete = blobs - set(collections.ALL.keys())
        print_items('Blob sets', blobs_to_delete)

        if not es_to_delete and not db_to_delete and not blobs_to_delete:
            print('Nothing to delete.')
            return

        if options.get('force') or confirm():
            for index in es_to_delete:
                print(f'\nDeleting index "{index}"...')
                indexing.delete_index_by_name(index)

            for db in db_to_delete:
                print(f'\nDeleting database "{db}"...')
                collections.drop_db(db)

            for blob_dir in blobs_to_delete:
                print(f'\nDeleting blob directory "{blob_dir}"...')
                shutil.rmtree(os.path.join(settings.SNOOP_BLOB_STORAGE, blob_dir))
        else:
            print('Exiting without any changes.\n')
            return
