from django.core.management.base import BaseCommand

from ... import collections
from ...logs import logging_for_management_command


class Command(BaseCommand):
    help = "Create and migrate the collection databases"

    def handle(self, *args, **options):
        logging_for_management_command(options['verbosity'])
        collections.create_databases()
        collections.migrate_databases()
        collections.create_es_indexes()
        collections.create_blob_roots()
