"""Entrypoint for worker process.

Starts up a variable number of worker processes with Celery, depending on settings and available CPU count.
"""

import os
import logging
import subprocess

from django.conf import settings
from django.core.management.base import BaseCommand

from snoop.profiler import Profiler
from snoop.data.collections import ALL

from ... import tasks
from ...logs import logging_for_management_command

log = logging.getLogger(__name__)


def celery_argv(queues, count, mem_limit_mb):
    """Builds the command line to run a `celery worker` process."""

    celery_binary = (
        subprocess.check_output(['which', 'celery'])
        .decode('latin1')
        .strip()
    )

    loglevel = 'info' if settings.DEBUG else 'warning'
    argv = [
        celery_binary,
        '-A', 'snoop.data',
        'worker',
        '-E',
        '--pidfile=',
        f'--loglevel={loglevel}',
        '-Ofair',
        '--max-tasks-per-child', str(settings.WORKER_TASK_LIMIT),
        '--max-memory-per-child', str(mem_limit_mb * 1024),
        '--prefetch-multiplier', str(14),
        '--soft-time-limit', '190000',  # 52h
        '--time-limit', '200000',  # 55h
        '-Q', ','.join(queues),
        '-c', str(count),
    ]

    return argv


class Command(BaseCommand):
    "Run celery worker"

    def add_arguments(self, parser):
        """Adds flag to switch between running collection workers and system workers."""
        parser.add_argument('--queue', default='default',
                            help="Run specific queue.")
        parser.add_argument('--count', type=int, default=1,
                            help="Worker processes to run (default 1).")
        parser.add_argument('--mem', type=int, default=500,
                            help=("If task exceeds this memory usage (in MB), "
                                  "after finishing, it will restart."))

    def handle(self, *args, **options):
        """Runs workers for either collection processing or system tasks."""

        logging_for_management_command()
        with Profiler():
            tasks.import_snoop_tasks()

            if options['queue'] == 'system':
                all_queues = settings.SYSTEM_QUEUES
            elif options['queue']:
                all_queues = [c.queue_name + '.' + options['queue'] for c in ALL.values()]
            else:
                raise RuntimeError('no queue given')

            argv = celery_argv(queues=all_queues, count=options['count'], mem_limit_mb=options['mem'])
            log.info('+' + ' '.join(argv))
            os.execv(argv[0], argv)
