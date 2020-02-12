import os
import logging
import subprocess

from django.conf import settings
from django.core.management.base import BaseCommand
from django.template.loader import render_to_string

from snoop.profiler import Profiler

from ... import tasks
from ...logs import logging_for_management_command

log = logging.getLogger(__name__)


def celery_argv(queues):
    celery_binary = (
        subprocess.check_output(['which', 'celery'])
        .decode('latin1')
        .strip()
    )

    argv = [
        celery_binary,
        '-A', 'snoop.data',
        '-E',
        '--pidfile=',
        '--loglevel=info',
        'worker',
        '-Ofair',
        '--max-tasks-per-child', str(settings.WORKER_TASK_LIMIT),
        '-Q', ','.join(queues),
        '-c', str(settings.WORKER_COUNT),
    ]

    return argv


def create_procfile(celery_args):
    with open('Procfile', 'w') as procfile:
        out = render_to_string('snoop/Procfile', context={'workers_command': ' '.join(celery_args)})
        procfile.write(out)

    with open('Procfile') as procfile:
        for line in procfile.readlines():
            log.info('Procfile: %s', line.strip())
    log.info('Procfile done.')


class Command(BaseCommand):
    help = "Run celery worker"

    def add_arguments(self, parser):
        parser.add_argument('func', nargs='*',
                            help="Task types to run")
        parser.add_argument('-p', '--prefix',
                            help="Prefix to insert to the queue name")

    def handle(self, *args, **options):
        logging_for_management_command()
        with Profiler():
            tasks.import_shaormas()
            if options.get('prefix'):
                prefix = options['prefix']
                settings.TASK_PREFIX = prefix
            else:
                prefix = settings.TASK_PREFIX
            queues = options.get('func') or tasks.shaormerie
            system_queues = ['populate_queue']

            argv = celery_argv(
                queues=[f'{prefix}.{queue}' for queue in queues] + system_queues,
            )
            create_procfile(argv)
            honcho_binary = (
                subprocess.check_output(['which', 'honcho'])
                .decode('latin1')
                .strip()
            )
            log.info('running honcho...')
            os.execv(honcho_binary, [honcho_binary, 'start'])
