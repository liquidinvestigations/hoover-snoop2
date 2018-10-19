from django.core.management.base import BaseCommand
from ...logs import logging_for_management_command
from ... import exportimport


class Command(BaseCommand):
    help = "Import database records"

    def handle(self, *args, **options):
        logging_for_management_command(options['verbosity'])
        exportimport.import_db()
