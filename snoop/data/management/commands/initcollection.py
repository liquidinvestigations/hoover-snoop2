from django.core.management.base import BaseCommand

from ...logs import logging_for_management_command


class Command(BaseCommand):
    help = "Initialize the collection database, index, and run dispatcher"

    def handle(self, *args, **options):
        logging_for_management_command(options['verbosity'])
        print("initcollection is now a no-op")
