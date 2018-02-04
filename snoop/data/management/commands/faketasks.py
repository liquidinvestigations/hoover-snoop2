from time import time
from django.core.management.base import BaseCommand
from ...logs import logging_for_management_command
from ... import models
from ...tasks import do_nothing


class Command(BaseCommand):
    help = "Creates fake tasks for benchmarking"

    def add_arguments(self, parser):
        parser.add_argument('number', type=int)
        parser.add_argument('--delete', action='store_true')

    def handle(self, *args, **options):
        logging_for_management_command()

        if options['delete']:
            models.Task.objects.filter(func='do_nothing').delete()

        t0 = time()
        for n in range(options['number']):
            do_nothing.laterz(f'{t0}-{n}')
