from django.core.management.base import BaseCommand
from ...logs import logging_for_management_command
from ... import models, collections
from ...tasks import retry_task


class Command(BaseCommand):
    help = "Retry running task"

    def add_arguments(self, parser):
        parser.add_argument('collection', help="collection name")
        parser.add_argument('task_pk', type=str, help="Primary key of a task for a retry.")
        parser.add_argument('--fg', action='store_true', help="Run task in foreground mode.")

    def handle(self, collection, task_pk, **options):
        logging_for_management_command()
        assert collection in collections.ALL, 'collection does not exist'
        with collections.ALL[collection].set_current():
            task = models.Task.objects.get(pk=task_pk)
            retry_task(task, fg=options['fg'])
