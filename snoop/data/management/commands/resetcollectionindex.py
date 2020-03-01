from django.core.management.base import BaseCommand

from ... import indexing
from ...logs import logging_for_management_command
from ...collections import Collection


class Command(BaseCommand):
    help = "Wipe and recreate the ElasticSearch index for a given collection"

    def add_arguments(self, parser):
        parser.add_argument('collection', type=str)

    def handle(self, collection, **options):
        logging_for_management_command(options['verbosity'])
        with Collection(collection).set_current():
            indexing.delete_index()
            indexing.create_index()
