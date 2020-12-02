import logging
import subprocess

from django.conf import settings
from django.core.management.base import BaseCommand

from ...logs import logging_for_management_command

log = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Make sure we have enough workers running in this container"

    def handle(self, *args, **options):
        logging_for_management_command()
        cmd = r"ps axo comm,args | grep '^celery .* snoop\.data.*worker' | wc -l"
        procs = int(subprocess.check_output(cmd, shell=True).decode())
        limit = settings.SNOOP_MIN_WORKERS
        log.info(f"running worker count on container: {procs}")
        log.info(f"out of min {limit}")
        assert procs >= limit, 'not enough workers'
