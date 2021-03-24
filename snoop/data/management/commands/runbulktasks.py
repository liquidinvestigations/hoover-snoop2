"""Runs a number of batches of bulk tasks.
"""
from django.core.management.base import BaseCommand
from ...tasks import run_bulk_tasks
from ...logs import logging_for_management_command


class Command(BaseCommand):
    """Runs a number of batches of bulk tasks.
    """

    def handle(self, *args, **options):
        logging_for_management_command()
        run_bulk_tasks()
