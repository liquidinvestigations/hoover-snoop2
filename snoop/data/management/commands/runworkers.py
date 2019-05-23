import os
import subprocess

from django.conf import settings
from django.core.management.base import BaseCommand
from django.template.loader import render_to_string

from snoop.profiler import Profiler

from ... import tasks

def bool_env(value):
    return (value or '').lower() in ['on', 'true']

def celery_argv(queues):
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
        '--max-tasks-per-child', '2000',
        '-Q', ','.join(queues),
        '-c', '1',  # single worker
    ]

    return argv


def create_procfile(celery_args):
    with open('Procfile', 'w') as procfile:
        out = render_to_string('snoop/Procfile', context={'workers_command': ' '.join(celery_args)})
        procfile.write(out)


class Command(BaseCommand):
    help = "Run celery worker"

    def add_arguments(self, parser):
        parser.add_argument('func', nargs='*',
                help="Task types to run")
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
            system_queues = ['watchdog']
            if bool_env(os.environ.get('SYNC_FILES')):
                system_queues += ['auto_sync']

            argv = celery_argv(
                queues=[f'{prefix}.{queue}' for queue in queues] + system_queues,
            )
            print('+', *argv)
            create_procfile(argv)
            honcho_binary = (
                subprocess.check_output(['which', 'honcho'])
                .decode('latin1')
                .strip()
            )
            os.execv(honcho_binary, [honcho_binary, 'start'])
