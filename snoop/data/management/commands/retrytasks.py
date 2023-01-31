"""Retry multiple tasks based on their function and status.

Optimized variant of [snoop.data.management.commands.retrytask][] for very long task lists (millions).
"""
from django.core.management.base import BaseCommand
from django.db import transaction
from ...logs import logging_for_management_command
from ... import models
from ... import tasks
from ... import collections


class Command(BaseCommand):
    "Re-queue all tasks that fit selection criteria."""

    def add_arguments(self, parser):
        """Arguments for the collection, and selection criteria: functions, statuses."""

        parser.add_argument('collection', type=str, help="collection name, or __ALL__ to run on all of them")
        parser.add_argument('--func', help="Filter by task function")
        parser.add_argument('--status', help="Filter by task status")
        parser.add_argument('--dry-run', action='store_true',
                            help="Don't run, just print number of tasks")

    def handle(self, collection, **options):
        """Runs [snoop.data.tasks.retry_tasks][] on the filtered tasks."""

        logging_for_management_command(options['verbosity'])

        tasks.import_snoop_tasks()

        if collection == '__ALL__':
            all_collections = list(collections.ALL.values())
        else:
            all_collections = [collections.ALL[collection]]

        for col in all_collections:
            with col.set_current():
                func = options.get('func')
                status = options.get('status')

                # assert status != models.Task.STATUS_PENDING, \
                #     "cannot use this on pending tasks"

                with transaction.atomic(using=collections.current().db_alias):
                    queryset = models.Task.objects.select_for_update(skip_locked=True)
                    if func:
                        queryset = queryset.filter(func=func)
                    if status:
                        queryset = queryset.filter(status=status)
                    # queryset = queryset.exclude(status=models.Task.STATUS_PENDING)
                    # queryset = queryset.order_by('date_modified')

                    if options.get('dry_run'):
                        print("Tasks to retry:", queryset.count())

                    else:
                        tasks.retry_tasks(queryset.all())
