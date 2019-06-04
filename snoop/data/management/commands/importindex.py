from django.core.management.base import BaseCommand
from ...logs import logging_for_management_command
from ... import indexing
import os


class Command(BaseCommand):
    help = "Import elasticsearch index"

    def add_arguments(self, parser):
        parser.add_argument('-d', '--delete', action='store_true',
                            help="Delete any existing index.")
        parser.add_argument('file_name', type=str, default=None, help="Export file")

    def handle(self, delete, file_name, *args, **options):
        logging_for_management_command(options['verbosity'])
        if file_name:
            with open(os.path.join('exports', f'{file_name}.tar'), 'rb') as import_file:
                indexing.import_index(delete=delete, stream=import_file)
        else:
            indexing.export_index(delete=delete)
