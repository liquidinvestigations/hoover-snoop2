from django.core.management.base import BaseCommand
from ...logs import logging_for_management_command
from ...magic import download_magic_definitions


class Command(BaseCommand):
    help = "Runs a task in foreground"

    def handle(self, *args, **options):
        logging_for_management_command()
        download_magic_definitions()
