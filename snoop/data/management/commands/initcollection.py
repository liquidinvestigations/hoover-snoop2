from django.core.management.base import BaseCommand
from django.core import management

from ... import indexing
from ...dispatcher import run_dispatcher
from ...logs import logging_for_management_command


class Command(BaseCommand):
    help = "Initialize the collection database, index, and run dispatcher"

    def handle(self, *args, **options):
        logging_for_management_command(options['verbosity'])
        management.call_command('migrate')
        indexing.create_index()
        run_dispatcher()
