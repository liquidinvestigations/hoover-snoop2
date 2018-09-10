import os
import subprocess

from django.core.management.base import BaseCommand

from snoop.data import models
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
        '-Q', ','.join(queues),
    ]

    if num_workers:
        argv += ['-c', num_workers if num_workers else os.cpu_count() * 2]

    return argv


class Command(BaseCommand):
    help = "Run celery worker"

    def add_arguments(self, parser):
        parser.add_argument('func', nargs='*',
                help="Task types to run")
        parser.add_argument('-n', '--num-workers',
                help="Number of workers to start")

    def handle(self, *args, **options):
        with Profiler():
            tasks.import_shaormas()
            queues = options.get('func') or tasks.shaormerie
            collection = [models.Collection.objects.all()]
            own_queues = [f'{collection.name}__{queue}' for queue in queues]
            argv = celery_argv(
                num_workers=options.get('num_workers'),
                queues=own_queues,
            )
            print('+', *argv)
            os.execv(argv[0], argv)
