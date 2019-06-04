from django.core.management.base import BaseCommand
from ...logs import logging_for_management_command
from ... import indexing
import os


class Command(BaseCommand):
    help = "Export elasticsearch index"

    def add_arguments(self, parser):
        parser.add_argument('file_name', type=str, default=None, help="Export file")

    def handle(self, file_name, *args, **options):
        logging_for_management_command(options['verbosity'])
        if file_name:
            with open(os.path.join('exports', f'{file_name}.tar'), 'w') as export_file:
                indexing.export_index(export_file)
        else:
            indexing.export_index()
