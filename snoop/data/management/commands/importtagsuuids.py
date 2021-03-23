"""Migration script for importing the user - UUID mapping from other service.

Warning:
    this is not useful anymore and should be deprecated and/or removed.
"""

import json
import sys
import logging

from django.core.management.base import BaseCommand
from snoop.data.logs import logging_for_management_command
from snoop.data import collections
from ... import models, tasks


log = logging.getLogger(__name__)


def fix(col, mapping):
    """Update the User UUIDs for a single collection."""

    log.info('> fixing collection %s', col.name)
    for username, uuid in mapping.items():
        q = models.DocumentUserTag.objects.filter(user=username).exclude(uuid=uuid)
        if not q.exists():
            continue
        log.info('>> changing %s tags for user "%s"', q.count(), username)
        q.update(uuid=uuid)

    digests_q = models.DocumentUserTag.objects.values('digest').distinct()
    digest_ids = models.Digest.objects.filter(id__in=digests_q).values('blob')

    task_qs = models.Task.objects \
        .filter(func='digests.index') \
        .exclude(status=models.Task.STATUS_PENDING) \
        .filter(blob_arg__in=digest_ids)

    log.info('retrying %s handle_file tasks', task_qs.count())
    tasks.retry_tasks(task_qs)


class Command(BaseCommand):
    "Import Tags UUIDs for all collections. JSON content is read from stdin."

    def handle(self, **options):
        logging_for_management_command(options['verbosity'])

        mapping = json.load(sys.stdin)

        for col in collections.ALL.values():
            with col.set_current():
                fix(col, mapping)
