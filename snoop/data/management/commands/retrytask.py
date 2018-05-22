import sys
from time import time
from django.core.management.base import BaseCommand
from ...logs import logging_for_management_command
from ... import models
from ...tasks import retry_task


class Command(BaseCommand):
    help = "Retry running task"

    def add_arguments(self, parser):
        parser.add_argument('task_pk', type=str)
        parser.add_argument('--fg', action='store_true')

    def handle(self, *args, **options):
        logging_for_management_command()
        task = models.Task.objects.get(pk=options['task_pk'])
        retry_task(task, fg=options['fg'])
