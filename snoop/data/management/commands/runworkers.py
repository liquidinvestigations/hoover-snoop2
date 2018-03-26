import os
import subprocess
from django.core.management.base import BaseCommand
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
        argv += ['-c', num_workers]

    return argv


class Command(BaseCommand):
    help = "Run celery worker"

    def add_arguments(self, parser):
        parser.add_argument('func', nargs='*',
                help="Task types to run")
        parser.add_argument('-n', '--num-workers',
                help="Number of workers to start")

    def handle(self, *args, **options):
        tasks.import_shaormas()
        queues = options.get('func') or tasks.shaormerie
        argv = celery_argv(
            num_workers=options.get('num_workers'),
            queues=queues,
        )
        print('+', *argv)
        os.execv(argv[0], argv)
