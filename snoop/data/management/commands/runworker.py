import os
import subprocess
from django.core.management.base import BaseCommand
from ... import tasks


def celery_argv():
    celery_binary = (
        subprocess.check_output(['which', 'celery'])
        .decode('latin1')
        .strip()
    )

    return [
        celery_binary,
        '-A', 'snoop.data',
        '--loglevel=info',
        'worker',
        '-Q', ','.join(tasks.shaormerie),
    ]


class Command(BaseCommand):
    help = "Run celery worker"

    def handle(self, *args, **options):
        tasks.import_shaormas()
        argv = celery_argv()
        print('+', *argv)
        os.execv(argv[0], argv)
