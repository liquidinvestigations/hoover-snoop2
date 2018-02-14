from django.core.management.base import BaseCommand
from ...logs import logging_for_management_command
from ... import indexing


class Command(BaseCommand):
    help = "Updates the elasticsearch index"

    def handle(self, *args, **options):
        logging_for_management_command(options['verbosity'])
        indexing.update()
