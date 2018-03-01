import os
import subprocess
from django.core.management.base import BaseCommand
from ... import tasks


def celery_argv(num_workers=None):
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
        '-Q', ','.join(tasks.shaormerie),
    ]

    if num_workers:
        argv += ['-c', num_workers]

    return argv


class Command(BaseCommand):
    help = "Run celery worker"

    def add_arguments(self, parser):
        parser.add_argument('-n', '--num-workers',
                help="Number of workers to start")

    def handle(self, *args, **options):
        tasks.import_shaormas()
        argv = celery_argv(num_workers=options.get('num_workers'))
        print('+', *argv)
        os.execv(argv[0], argv)
