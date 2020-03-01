import sys
from django.core.management.base import BaseCommand
from ...logs import logging_for_management_command
from ... import models
from ... import collections


class Command(BaseCommand):
    help = "Check if there are tasks to be dispatched"

    def add_arguments(self, parser):
        parser.add_argument('collection', type=str)

    def handle(self, collection, **options):
        logging_for_management_command(options['verbosity'])

        col = collections.ALL[collection]
        with col.set_current():
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
