"""Creates and migrates databases and indexes.
"""

from django.core.management.base import BaseCommand

from ... import collections
from ...logs import logging_for_management_command


class Command(BaseCommand):
    help = "Create and migrate the collection databases"

    def add_arguments(self, parser):
        parser.add_argument('options', nargs='*', type=str, help='args passed to django migrate')

    def handle(self, *args, **options):
        logging_for_management_command(options['verbosity'])
        collections.create_databases()
        collections.migrate_databases(*options['options'])
        collections.create_es_indexes()
        collections.create_roots()
