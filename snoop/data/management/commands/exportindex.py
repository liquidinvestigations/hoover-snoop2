"""Export elasticsearch index."""

from django.core.management.base import BaseCommand
from ...logs import logging_for_management_command
from ... import indexing


class Command(BaseCommand):
    """Export elasticsearch index"""

    def handle(self, *args, **options):
        """Runs [snoop.data.indexing.export_index][].

        Warning:
            This probably requires an update, since there's no option to select a collection.
        """

        logging_for_management_command(options['verbosity'])
        indexing.export_index()
