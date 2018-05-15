from django.core.management.base import BaseCommand
from ...logs import logging_for_management_command
from ... import indexing
from ... import models


class Command(BaseCommand):
    help = "Wipe and recreate the ElasticSearch index for a given collection"

    def add_arguments(self, parser):
        parser.add_argument('collection_name', type=str)

    def handle(self, collection_name, *args, **options):
        logging_for_management_command(options['verbosity'])
        collection = models.Collection.objects.get(name=collection_name)
        indexing.delete_index(collection.name)
        indexing.create_index(collection.name)
