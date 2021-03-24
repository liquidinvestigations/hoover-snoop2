"""Update all the tags.

May be required when doing data migrations.
"""

from django.core.management.base import BaseCommand

from snoop.data.logs import logging_for_management_command

from ... import tasks


class Command(BaseCommand):
    """Update all tags."""
    help = "Update all tags"

    def handle(self, *args, **options):
        """Runs [snoop.data.tasks.update_all_tags][],"""

        logging_for_management_command(options['verbosity'])
        tasks.update_all_tags()
