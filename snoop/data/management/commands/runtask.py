"""Runs task in the foreground.

Equivalent of using [snoop.data.management.commands.retrytasks][].
"""

from django.core.management.base import BaseCommand
from ...logs import logging_for_management_command
from ... import tasks


class Command(BaseCommand):
    "Runs a task in foreground"

    def add_arguments(self, parser):
        parser.add_argument('collection', type=str)
        parser.add_argument('task_pk', type=int)

    def handle(self, *args, collection, task_pk, **options):
        logging_for_management_command()
        tasks.run_task_with_lock(collection, task_pk, raise_exceptions=True)
