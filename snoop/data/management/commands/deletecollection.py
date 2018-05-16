from django.core.management.base import BaseCommand
from ...logs import logging_for_management_command
from ... import collections


class Command(BaseCommand):
    help = "Deletes a collection"

    def add_arguments(self, parser):
        parser.add_argument('name')

    def handle(self, *args, **options):
        logging_for_management_command(options['verbosity'])
        collections.delete_collection(options['name'])
