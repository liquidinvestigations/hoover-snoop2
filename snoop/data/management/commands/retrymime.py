import datetime

import logging
from django.db.models import F
from django.db.models.expressions import RawSQL
from django.core.management.base import BaseCommand
from django.utils.timezone import make_aware

from ...logs import logging_for_management_command
from ... import models
from ... import tasks
from ... import collections

log = logging.getLogger(__name__)

INTERESTING_MIME_TYPES = [
    'application/CDFV2',
    'application/vnd.ms-excel',
]


def fix(col, dry_run, not_after=None):
    log.info('=' * 30)
    log.info("fixing XLS-related mime type issues in collection %s", col)
    if not_after:
        log.warning('resuming with tasks started before %s', not_after)
    if dry_run:
        log.info("(dry run only!)")
    with col.set_current():
        # remove stale filesystem.handle_file dependencies
        task_qs = models.Task.objects.filter(func='email.msg_to_eml', status='error') \
            | models.Task.objects.filter(func='email.msg_to_eml', status='broken')
        log.info('removing %s outdated msg_to_eml tasks', task_qs.count())
        if not dry_run:
            task_qs.delete()

        blob_qs = models.Blob.objects.filter(mime_encoding=F('magic')) \
            | models.Blob.objects.filter(mime_type__in=INTERESTING_MIME_TYPES)

        log.info('updating magic info for %s blobs', blob_qs.count())
        for b in blob_qs.iterator():
            log.debug('updating magic info for blob %s', b.pk)
            if not dry_run:
                b.update_magic()

        file_qs = models.File.objects.exclude(original=F('blob')) \
            | models.File.objects.filter(original__mime_encoding=F('original__magic')) \
            | models.File.objects.filter(blob__mime_encoding=F('blob__magic')) \
            | models.File.objects.filter(name_bytes__endswith=b'.xls') \
            | models.File.objects.filter(original__mime_type__in=INTERESTING_MIME_TYPES)

        task_qs = models.Task.objects \
            .filter(func='filesystem.handle_file') \
            .exclude(status=models.Task.STATUS_PENDING) \
            .annotate(pkint=RawSQL("((args->0)::numeric)", ())) \
            .filter(pkint__in=file_qs.values('pk'))
        if not_after:
            task_qs = task_qs.filter(date_started__lt=not_after)

        log.info('retrying %s handle_file tasks', task_qs.count())
        for task in task_qs.iterator():
            if dry_run:
                log.info("Task to retry: %s", task)

            else:
                tasks.retry_task(task)


class Command(BaseCommand):
    help = "Retry running task"

    def add_arguments(self, parser):
        parser.add_argument('collection_names', type=str, nargs='+',
                            help="set to ALL to run on all collections")
        parser.add_argument('--started-before',
                            type=lambda s: make_aware(datetime.datetime.strptime(s, '%Y-%m-%d')),
                            help="Y-m-d format date to resume after a previously interrupted attempt")  # noqa: E501
        parser.add_argument('--dry-run', action='store_true',
                            help="Don't run, just print number of tasks")

    def handle(self, collection_names, dry_run, started_before, **options):
        logging_for_management_command(options['verbosity'])

        if 'ALL' in collection_names:
            for col in collections.ALL.values():
                fix(col, dry_run, started_before)
            return
        for name in collection_names:
            col = collections.ALL[name]
            fix(col, dry_run, started_before)
