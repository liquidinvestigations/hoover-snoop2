from django.core.management.base import BaseCommand

from ... import indexing
from ...logs import logging_for_management_command


class Command(BaseCommand):
    help = "Initialize the collection database, index, and run dispatcher"

    def handle(self, *args, **options):
        logging_for_management_command(options['verbosity'])
        # try and create index if it doesn't exist this is required for backing
        # up collections which have never been started.
        try:
            indexing.create_index()
        except RuntimeError:
            # already created?
            pass
