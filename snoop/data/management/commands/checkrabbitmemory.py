"""Debug command to check rabbitMQ memory for snoop.

Prints result to stdout; script always returns 0."""
import logging

from django.core.management.base import BaseCommand

from ... import tasks
from ...logs import logging_for_management_command

log = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        """Debug command to check rabbitMQ memory for snoop."""

        logging_for_management_command()
        print('rabbitmq memory full: ', tasks._is_rabbitmq_memory_full())
