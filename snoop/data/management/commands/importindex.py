from django.core.management.base import BaseCommand
from ...logs import logging_for_management_command
from ... import indexing


class Command(BaseCommand):
    help = "Import elasticsearch index"

    def add_arguments(self, parser):
        parser.add_argument('-d', '--delete', action='store_true',
                            help="Delete any existing index.")

    def handle(self, delete, *args, **options):
        logging_for_management_command(options['verbosity'])
        indexing.import_index(delete=delete)
