import os
import subprocess

from django.conf import settings
from django.core.management.base import BaseCommand
from django.template.loader import render_to_string

from snoop.profiler import Profiler

from ... import tasks


def celery_argv(custom_workers_no, queues):
    workers_multiplier = 1
    cpu_count = os.cpu_count()
    max_workers_no = min(int(cpu_count * 1.5), 100)

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
        '--max-tasks-per-child', '500',
        '-Q', ','.join(queues),
    ]

    workers_no = int(custom_workers_no) if custom_workers_no else int(cpu_count * workers_multiplier)
    if workers_no > max_workers_no:
        print(f'Limitting the number of workers to {max_workers_no} on {cpu_count} CPUs.')
        workers_no = max_workers_no
    else:
        print(f'Starting with {workers_no} workers on {cpu_count} CPUs.')

    argv += ['-c', str(workers_no)]

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
        parser.add_argument('-n', '--workers-no',
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
                custom_workers_no=options.get('workers_no'),
                queues=[f'{prefix}.{queue}' for queue in queues] + ['watchdog'],
            )
            print('+', *argv)
            create_procfile(argv)
            honcho_binary = (
                subprocess.check_output(['which', 'honcho'])
                .decode('latin1')
                .strip()
            )
            os.execv(honcho_binary, [honcho_binary, 'start'])
#             os.execv(argv[0], argv)
