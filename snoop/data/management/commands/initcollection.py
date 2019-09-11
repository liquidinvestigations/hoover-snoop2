from django.core.management.base import BaseCommand

from .magic import download_magic_definitions
from ... import indexing, models
from ...logs import logging_for_management_command


class Command(BaseCommand):
    help = "Initialize the collection database, index, and run dispatcher"

    def handle(self, *args, **options):
        logging_for_management_command(options['verbosity'])

        download_magic_definitions()

        if not models.Directory.root():
            models.Directory.objects.create()

        indexing.create_index()
