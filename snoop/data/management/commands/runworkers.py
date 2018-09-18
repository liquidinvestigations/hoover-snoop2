import os
import subprocess

from django.conf import settings
from django.core.management.base import BaseCommand

from snoop.profiler import Profiler

from ... import tasks


def celery_argv(num_workers, queues):
    celery_binary = (
        subprocess.check_output(['which', 'celery'])
        .decode('latin1')
        .strip()
    )

    argv = [
        celery_binary,
        '-A', 'snoop.data',
        '--loglevel=info',
        'worker',
        '-Ofair',
        '--max-tasks-per-child', '1000',
        '-Q', ','.join(queues),
    ]

    argv += ['-c', num_workers if num_workers else str(os.cpu_count() * 4)]

    return argv


class Command(BaseCommand):
    help = "Run celery worker"

    def add_arguments(self, parser):
        parser.add_argument('func', nargs='*',
                help="Task types to run")
        parser.add_argument('-n', '--num-workers',
                help="Number of workers to start")
        parser.add_argument('-p', '--prefix',
                help="Prefix to insert to the queue name")

    def handle(self, *args, **options):
        with Profiler():
            tasks.import_shaormas()
            if options.get('prefix'):
                prefix = options['prefix']
                settings.TASK_PREFIX = prefix
            else:
                prefix = settings.TASK_PREFIX
            queues = options.get('func') or tasks.shaormerie
            argv = celery_argv(
                num_workers=options.get('num_workers'),
                queues=[f'{prefix}.{queue}' for queue in queues],
            )
            print('+', *argv)
            os.execv(argv[0], argv)
