"""Delete database from postgres, making sure to drop connections.

This command is required because postgres 12 does not have `dropdb --force`.
It was added in version 13, which we do not have yet.
"""
import logging

from django.core.management.base import BaseCommand
from ...logs import logging_for_management_command

from ... import collections

log = logging.getLogger(__name__)


class Command(BaseCommand):
    "Retry running task"

    def add_arguments(self, parser):
        parser.add_argument('db_name', help="databasee name")

    def handle(self, db_name, **options):
        """Find and delete databases, indexes and blobs not bound to any collection.
        """
        logging_for_management_command(options['verbosity'])

        collections.drop_db(db_name)
