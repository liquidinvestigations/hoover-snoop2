from django.core.management.base import BaseCommand

from snoop.data.logs import logging_for_management_command

from ... import tasks


class Command(BaseCommand):
    help = "Update all tags"

    def handle(self, *args, **options):
        logging_for_management_command(options['verbosity'])
        tasks.update_all_tags()
