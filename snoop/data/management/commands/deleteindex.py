from django.core.management.base import BaseCommand
from ... import indexing
from ...logs import logging_for_management_command


class Command(BaseCommand):
    help = "deletes the elasticsearch-index of the given collection."

    def handle(self, *args, **options):
        logging_for_management_command(options['verbosity'])
        indexing.delete_index()
