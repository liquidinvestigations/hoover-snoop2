from django.core.management.base import BaseCommand
from ...logs import logging_for_management_command
from ... import indexing
import os


class Command(BaseCommand):
    help = "Import elasticsearch index"

    def add_arguments(self, parser):
        parser.add_argument('-d', '--delete', action='store_true',
                            help="Delete any existing index.")
        parser.add_argument('-i', '--index', nargs = 1,
                            help='Snapshot name (previous index name)')
        parser.add_argument('file_name', type=str, default=None, help="Export file")

    def handle(self, delete, index, file_name, *args, **options):
        index = index[0] if index else None
        logging_for_management_command(options['verbosity'])
        if file_name:
            file_name = file_name if file_name.endswith('.tar') else f'{file_name}.tar'
            with open(os.path.join('exports', file_name), 'rb') as import_file:
                indexing.import_index(delete=delete, stream=import_file,
                                      from_index=index)
        else:
            indexing.import_index(delete=delete, from_index=index)
