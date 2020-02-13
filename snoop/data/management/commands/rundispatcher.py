from django.core.management.base import BaseCommand
from ...tasks import run_dispatcher
from ...logs import logging_for_management_command


class Command(BaseCommand):
    help = (
        "Runs the dispatcher, which keeps collections up to date, by "
        "scanning the filesystem and launching processing jobs."
    )

    def handle(self, *args, **options):
        logging_for_management_command()
        run_dispatcher()
