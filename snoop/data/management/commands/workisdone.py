import sys
from django.core.management.base import BaseCommand
from ...logs import logging_for_management_command
from ... import models


class Command(BaseCommand):
    help = "Check if there are tasks to be dispatched"

    def handle(self, **options):
        logging_for_management_command(options['verbosity'])

        queryset = (
            models.Task.objects
            .filter(status__in=[
                models.Task.STATUS_PENDING,
                models.Task.STATUS_DEFERRED,
            ])
        )

        if queryset.first() is not None:
            print("There are some pending or deferred tasks")
            sys.exit(1)
        print("There are no pending or deferred tasks")
