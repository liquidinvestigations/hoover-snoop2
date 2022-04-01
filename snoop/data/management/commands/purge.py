"""Delete old databases, indexes and blobs.

This command is the only supported way of removing data from Snoop, one collection at a time.
"""

from django.core.management.base import BaseCommand
from django.conf import settings
from ...logs import logging_for_management_command

from ... import indexing
from ... import collections


PROMPT = 'ALL data printed above will be DELETED (type "yes" to confirm):'


def confirm():
    return input(PROMPT).strip().lower() == 'yes'


class Command(BaseCommand):
    "Retry running task"

    def add_arguments(self, parser):
        """One flag called `--force` to avoid being asked for confirmation."""

        parser.add_argument('--force', action='store_true',
                            help="Don't ask for confirmation")

    def handle(self, **options):
        """Find and delete databases, indexes and blobs not bound to any collection.
        """

        logging_for_management_command(options['verbosity'])

        def print_items(name, items):
            if not items:
                print(f'{name} to delete: none')
            else:
                print()
                print(f'{name} to delete {len(items)} items:')
                for item in items:
                    print('  - ', item)
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

        blob_buckets = set([b.name for b in settings.BLOBS_S3.list_buckets()])
        active_buckets = set(c.name for c in collections.ALL.values())
        buckets_to_delete = blob_buckets - active_buckets
        print_items('Minio/S3 Buckets (blob storage)', buckets_to_delete)

        if not es_to_delete and not db_to_delete and not buckets_to_delete:
            print('Nothing to delete.')
            return

        if options.get('force') or confirm():
            for index in es_to_delete:
                print(f'\nDeleting index "{index}"...')
                indexing.delete_index_by_name(index)

            for db in db_to_delete:
                print(f'\nDeleting database "{db}"...')
                collections.drop_db(db)

            for bucket in buckets_to_delete:
                print(f'\nDeleting S3 bucket "{bucket}"...')
                settings.BLOBS_S3.remove_bucket(bucket)

        else:
            print('Exiting without any changes.\n')
            return
