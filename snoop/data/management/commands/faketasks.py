"""Create a large number of "fake" tasks to use for benchmarking the Tasks system.
"""

from time import time
from django.core.management.base import BaseCommand
from ...logs import logging_for_management_command
from ... import models
from ...tasks import do_nothing


class Command(BaseCommand):
    """Creates fake tasks for benchmarking"""

    help = "Creates fake tasks for benchmarking"

    def add_arguments(self, parser):
        """Argument for task count, and flags for deleting old tasks."""

        parser.add_argument('number', type=int, help="Number of fake tasks to create.")
        parser.add_argument('--delete', action='store_true',
                            help="Delete existing fake tasks first.")

    def handle(self, *args, **options):
        logging_for_management_command()

        if options['delete']:
            models.Task.objects.filter(func='do_nothing').delete()

        t0 = time()
        for n in range(options['number']):
            do_nothing.laterz(f'{t0}-{n}')
