import logging
from django.db.models import F
from django.core.management.base import BaseCommand
from ...logs import logging_for_management_command
from ... import models
from ... import tasks
from ... import collections

log = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Retry running task"

    def add_arguments(self, parser):
        parser.add_argument('collection', type=str)
        parser.add_argument('--dry-run', action='store_true',
                            help="Don't run, just print number of tasks")

    def handle(self, collection, **options):
        logging_for_management_command(options['verbosity'])

        col = collections.ALL[collection]
        with col.set_current():
            file_qs = models.File.objects.exclude(original=F('blob')).values('pk')
            for i in file_qs.iterator():
                f_pk = i['pk']
                task_qs = models.Task.objects \
                    .filter(func='filesystem.handle_file') \
                    .filter(args__0=f_pk)
                try:
                    task = task_qs.get()

                    if options.get('dry_run'):
                        print("Task to retry:", task)

                    else:
                        tasks.retry_task(task)
                except models.Task.DoesNotExist:
                    log.error('task not found for file id: ' + str(f_pk))
