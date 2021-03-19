"""Update a collection's name"""

import sys
from django.core.exceptions import ObjectDoesNotExist
from django.core.management.base import BaseCommand

from snoop.data.logs import logging_for_management_command

from ... import models


class Command(BaseCommand):
    "Update the collection name"

    def add_arguments(self, parser):
        """Positional arguments -- old name, new name."""

        parser.add_argument('collection_name', type=str, help="Existing collection name.")
        parser.add_argument('new_collection_name', type=str, help="Unique collection name.")

    def handle(self, collection_name, new_collection_name, *args, **options):
        logging_for_management_command(options['verbosity'])
        try:
            collection = models.Collection.objects.get(name=collection_name)
            collection.name = new_collection_name
            collection.save()
        except ObjectDoesNotExist:
            try:
                collection = models.Collection.objects.get(name=new_collection_name)
            except ObjectDoesNotExist:
                print('Invalid collection name %s' % collection_name)
                sys.exit(1)
