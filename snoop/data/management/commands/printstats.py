from django.core.management.base import BaseCommand
from snoop.data.logs import logging_for_management_command
from snoop.data.admin import get_stats


class Command(BaseCommand):
    help = "Print task stats"

    def handle(self, **options):
        logging_for_management_command(options['verbosity'])
        for x in sorted(get_stats()['task_matrix']):
            print(x)
