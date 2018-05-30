from django.core.management.base import BaseCommand
from ...logs import logging_for_management_command
from ... import indexing


class Command(BaseCommand):
    help = "Import elasticsearch index for a collection"

    def add_arguments(self, parser):
        parser.add_argument('collection_name', type=str, help="Unique collection name.")
        parser.add_argument('-d', '--delete', action='store_true', help="Delete collection first.")

    def handle(self, collection_name, delete, *args, **options):
        logging_for_management_command(options['verbosity'])
        indexing.import_index(collection_name, delete=delete)
