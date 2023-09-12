"""Sync collection data with the common database.
"""
from django.core.management.base import BaseCommand
from ...tasks import sync_common_data
from ...logs import logging_for_management_command


class Command(BaseCommand):
    """Sync collection data with the common database.
    """

    def handle(self, *args, **options):
        logging_for_management_command()
        sync_common_data()
