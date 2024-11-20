"""Wipe and recreate ES index.
"""

from django.core.management.base import BaseCommand

from ... import indexing
from ...logs import logging_for_management_command


class Command(BaseCommand):
    "Wipe and recreate the ElasticSearch index for a given collection"

    def add_arguments(self, parser):
        parser.add_argument('collection', type=str)

    def handle(self, collection, **options):
        logging_for_management_command(options['verbosity'])
        indexing.delete_index_by_name(collection)
        indexing.create_index_by_name(collection)
