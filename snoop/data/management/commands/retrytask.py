import sys
from time import time
from django.core.management.base import BaseCommand
from ...logs import logging_for_management_command
from ... import models
from ...tasks import retry_tasks


class Command(BaseCommand):
    help = "Retry running task"

    def add_arguments(self, parser):
        parser.add_argument('task_pk', type=str)

    def handle(self, *args, **options):
        logging_for_management_command()
        retry_tasks(models.Task.objects.filter(pk=options['task_pk']))
