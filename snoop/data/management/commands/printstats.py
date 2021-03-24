"""Print table with task counts.
"""

import logging

from django.core.management.base import BaseCommand
from snoop.data.logs import logging_for_management_command
from snoop.data.admin import get_stats
from snoop.data import collections

log = logging.getLogger(__name__)


class Command(BaseCommand):
    """Print task stats."""

    help = "Print task stats"

    def add_arguments(self, parser):
        """Only argument is the collection.
        """
        parser.add_argument('collection', type=str)

    def handle(self, collection, **options):
        """Runs [snoop.data.admin.get_stats][] and prints result.
        """

        logging_for_management_command(options['verbosity'])
        col = collections.ALL[collection]
        with col.set_current():
            stats = sorted(get_stats()['task_matrix'])
            if not stats:
                log.warning('no tasks found')

            for x in stats:
                print(x)
