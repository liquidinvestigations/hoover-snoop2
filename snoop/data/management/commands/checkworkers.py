"""Command to check health of running workers.
"""

import logging
import subprocess

from django.conf import settings
from django.core.management.base import BaseCommand

from ...logs import logging_for_management_command

log = logging.getLogger(__name__)


class Command(BaseCommand):
    """Health check looking at worker process count.

    Will fail if we have less than [snoop.defaultsettings.SNOOP_MIN_WORKERS][] workers running on this node.
    Uses good old `ps` to get process count, then compares with the value above.
    """

    help = "Make sure we have enough workers running in this container"

    def handle(self, *args, **options):
        logging_for_management_command()
        cmd = r"ps axo comm,args | grep '^celery .* snoop\.data.*worker' | wc -l"
        procs = int(subprocess.check_output(cmd, shell=True).decode())
        limit = settings.SNOOP_MIN_WORKERS
        log.info(f"running worker count on container: {procs}")
        log.info(f"out of min {limit}")
        assert procs >= limit, 'not enough workers'
