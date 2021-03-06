from django.core.management.base import BaseCommand
from ...logs import logging_for_management_command
from ... import models
from ... import tasks
from ... import collections


class Command(BaseCommand):
    help = "Retry running task"

    def add_arguments(self, parser):
        parser.add_argument('collection', type=str)
        parser.add_argument('--func', help="Filter by task function")
        parser.add_argument('--status', help="Filter by task status")
        parser.add_argument('--dry-run', action='store_true',
                            help="Don't run, just print number of tasks")

    def handle(self, collection, **options):
        logging_for_management_command(options['verbosity'])

        col = collections.ALL[collection]
        with col.set_current():
            func = options.get('func')
            status = options.get('status')

            # assert status != models.Task.STATUS_PENDING, \
            #     "cannot use this on pending tasks"

            queryset = models.Task.objects
            if func:
                queryset = queryset.filter(func=func)
            if status:
                queryset = queryset.filter(status=status)
            # queryset = queryset.exclude(status=models.Task.STATUS_PENDING)
            queryset = queryset.order_by('date_modified')

            if options.get('dry_run'):
                print("Tasks to retry:", queryset.count())

            else:
                tasks.retry_tasks(queryset)
