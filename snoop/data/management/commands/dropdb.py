"""Delete database for specific collection.

This command is required because postgres 12 does not have `dropdb --force`.
It was added in version 13, which we do not have yet.
"""
import logging

from django.core.management.base import BaseCommand
from ...logs import logging_for_management_command

from ... import collections

log = logging.getLogger(__name__)


def confirm(PROMPT):
    return input(PROMPT).strip().lower() == 'yes'


class Command(BaseCommand):
    "Retry running task"

    def add_arguments(self, parser):
        """One flag called `--force` to avoid being asked for confirmation."""

        parser.add_argument('db_name', help="databasee name")
        parser.add_argument('--force', action='store_true',
                            help="Don't ask for confirmation")

    def handle(self, db_name, force, **options):
        """Find and delete databases, indexes and blobs not bound to any collection.
        """

        logging_for_management_command(options['verbosity'])

        dbs = set(collections.all_collection_dbs())
        if db_name not in dbs:
            log.warning('no datbase to drop!')
            return

        PROMPT = f'POSTGRES DATABASE {db_name} WILL BE DROPPED (type "yes" to confirm):'
        if options.get('force') or confirm(PROMPT):
            collections.drop_db(db_name)
        else:
            print('Exiting without any changes.\n')
            return
