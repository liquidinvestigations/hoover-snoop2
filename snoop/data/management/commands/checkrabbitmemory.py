import logging

from django.core.management.base import BaseCommand

from ... import tasks
from ...logs import logging_for_management_command

log = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        """Runs workers for either collection processing or system tasks."""

        logging_for_management_command()
        print('rabbitmq memory full: ', tasks._is_rabbitmq_memory_full())
