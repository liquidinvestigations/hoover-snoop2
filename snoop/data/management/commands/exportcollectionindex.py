from django.core.management.base import BaseCommand
from ...logs import logging_for_management_command
from ... import indexing


class Command(BaseCommand):
    help = "Export elasticsearch index for a collection"

    def add_arguments(self, parser):
        parser.add_argument('collection_name', type=str, help="Unique collection name.")

    def handle(self, collection_name, *args, **options):
        logging_for_management_command(options['verbosity'])
        indexing.export_index(collection_name)
