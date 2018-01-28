from django.core.management.base import BaseCommand
from ...logs import logging_for_management_command
from ... import tasks


class Command(BaseCommand):
    help = "Runs a task in foreground"

    def add_arguments(self, parser):
        parser.add_argument('task_pk', type=int)

    def handle(self, *args, task_pk, **options):
        logging_for_management_command()
        tasks.laterz_shaorma(task_pk, raise_exceptions=True)
