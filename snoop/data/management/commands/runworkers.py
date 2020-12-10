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


def celery_argv(queues):
    celery_binary = (
        subprocess.check_output(['which', 'celery'])
        .decode('latin1')
        .strip()
    )

    loglevel = 'warning' if settings.DEBUG else 'error'
    argv = [
        celery_binary,
        '-A', 'snoop.data',
        'worker',
        '-E',
        '--pidfile=',
        f'--loglevel={loglevel}',
        '-Ofair',
        '--max-tasks-per-child', str(settings.WORKER_TASK_LIMIT),
        '--max-memory-per-child', str(settings.WORKER_MEMORY_LIMIT * 1024),
        '--prefetch-multiplier', str(14),
        '--soft-time-limit', '190000',  # 52h
        '--time-limit', '200000',  # 55h
        '-Q', ','.join(queues),
        '-c', str(settings.WORKER_COUNT),
    ]

    return argv


class Command(BaseCommand):
    help = "Run celery worker"

    def handle(self, *args, **options):
        logging_for_management_command()
        with Profiler():
            tasks.import_snoop_tasks()

            all_queues = [c.queue_name for c in ALL.values()] + settings.SYSTEM_QUEUES
            argv = celery_argv(queues=all_queues)
            log.info('+' + ' '.join(argv))
            os.execv(argv[0], argv)
