from django.core.management.base import BaseCommand
from ...logs import logging_for_management_command
from ... import models
from ... import tasks


class Command(BaseCommand):
    help = "Retry running task"

    def add_arguments(self, parser):
        parser.add_argument('--func', help="Filter by task function")
        parser.add_argument('--status', help="Filter by task status")
        parser.add_argument('--dry-run', action='store_true',
                            help="Don't run, just print number of tasks")

    def handle(self, **options):
        logging_for_management_command(options['verbosity'])

        queryset = models.Task.objects

        func = options.get('func')
        if func:
            queryset = queryset.filter(func=func)

        status = options.get('status')
        if status:
            queryset = queryset.filter(status=status)

        if options.get('dry_run'):
            print("Tasks to retry:", queryset.count())

        else:
            tasks.retry_tasks(queryset)
