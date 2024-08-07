"""Entrypoint for worker process.

Starts up a variable number of worker processes with Celery, depending on settings and available CPU count.
"""

import os
import logging
import subprocess
import random

from django.conf import settings
from django.core.management.base import BaseCommand

from ... import tasks
from ...logs import logging_for_management_command

log = logging.getLogger(__name__)


def celery_argv(queues, solo, count, mem_limit_mb, name):
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
        '-n', name,
        '--pidfile=',
        f'--loglevel={loglevel}',
        '-Ofair',
        '--without-gossip', '--without-mingle',
        '--max-tasks-per-child', str(settings.WORKER_TASK_LIMIT),
        # '--max-tasks-per-child', str(1),
        '--max-memory-per-child', str(mem_limit_mb * 1024),
        '--prefetch-multiplier', str(settings.WORKER_PREFETCH),
        '--soft-time-limit', '216000',  # 60h
        '--time-limit', '230400',  # 64h
        '-Q', ','.join(queues),
    ]

    if solo:
        argv += ['-P', 'solo']
    else:
        argv += ['-P', 'prefork', '-c', str(count)]

    return argv


def rmq_queues_for(queue):
    """Return the rabbitmq complete queue names, given
    the queue category (the queue argument of @snoop_task).
    """
    lst = [
        tasks.rmq_queue_name(func)
        for func in tasks.task_map
        if tasks.task_map[func].queue == queue
    ]
    return list(set(lst))


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
        parser.add_argument('--solo', action="store_true",
                            help=("Run a single worker with celery solo pool."
                                  "Useful to kill container resources when task is killed."))

    def handle(self, *args, **options):
        """Runs workers for either collection processing or system tasks."""

        logging_for_management_command()

        tasks.import_snoop_tasks()

        all_queues = []
        if options['queue'] == 'system':
            all_queues = settings.SYSTEM_QUEUES
        elif options['queue'] == 'queues':
            all_queues.append(tasks.QUEUE_ANOTHER_TASK)
        elif options['queue']:
            all_queues.extend(rmq_queues_for(options['queue']))
            # every worker can run digests and filesystem and ocr (if enabled)
            all_queues.extend(rmq_queues_for('digests'))
            all_queues.extend(rmq_queues_for('filesystem'))
            all_queues.extend(rmq_queues_for('default'))

            if settings.OCR_ENABLED:
                all_queues.extend(rmq_queues_for('ocr'))

            if options['queue'] == 'default':
                all_queues.extend(rmq_queues_for('default'))
                all_queues.extend(rmq_queues_for('filesystem'))
                all_queues.extend(rmq_queues_for('ocr'))
                all_queues.extend(rmq_queues_for('digests'))

                all_queues.extend(rmq_queues_for('img-cls'))
                all_queues.extend(rmq_queues_for('entities'))
                all_queues.extend(rmq_queues_for('translate'))
                all_queues.extend(rmq_queues_for('thumbnails'))
                all_queues.extend(rmq_queues_for('pdf-preview'))

            all_queues.append(tasks.QUEUE_ANOTHER_TASK)
        else:
            raise RuntimeError('no queue given')

        all_queues = list(set(all_queues))
        random.shuffle(all_queues)

        worker_name = options['queue'] + str(random.randint(1, 10000)) + '@%h'
        argv = celery_argv(queues=all_queues, solo=options.get('solo'),
                           count=options['count'], mem_limit_mb=options['mem'],
                           name=worker_name)
        log.info('+' + ' '.join(argv))
        os.execv(argv[0], argv)
