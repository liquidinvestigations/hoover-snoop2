from django.core.management.base import BaseCommand

from snoop.data.analyzers.entities import dispatch_entity_detection
from ...logs import logging_for_management_command


class Command(BaseCommand):
    help = (
        "Runs the dispatcher for entity detection for all indexed files."
    )

    def handle(self, *args, **options):
        logging_for_management_command()
        dispatch_entity_detection()
