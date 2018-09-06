from pathlib import Path

from django.core.management.base import BaseCommand

from ... import collections
from ...logs import logging_for_management_command


class Command(BaseCommand):
    help = "Creates a collection"

    def add_arguments(self, parser):
        parser.add_argument('name', help="Unique collection name.")
        parser.add_argument('root', type=Path, help="A valid filesystem path.")

    def handle(self, *args, **options):
        logging_for_management_command(options['verbosity'])
        collections.create_collection(options['name'], options['root'])
