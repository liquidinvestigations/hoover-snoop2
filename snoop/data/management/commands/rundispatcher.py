"""Runs the dispatcher.
"""
from django.core.management.base import BaseCommand
from ...tasks import run_dispatcher
from ...logs import logging_for_management_command


class Command(BaseCommand):
    """Runs one iteration of the dispatcher.

    This keeps collections up to date by scanning the filesystem and launching processing jobs as
    required.

    Calls [snoop.data.tasks.run_dispatcher][].
    """

    def handle(self, *args, **options):
        logging_for_management_command()
        run_dispatcher()
