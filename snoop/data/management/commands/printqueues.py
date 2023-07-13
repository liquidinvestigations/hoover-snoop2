"""Print table with rabbitMQ queue depth.
"""

import random
import logging

from django.core.management.base import BaseCommand
from snoop.data.logs import logging_for_management_command
from snoop.data import collections
from snoop.data import tasks

log = logging.getLogger(__name__)


class Command(BaseCommand):
    """Print queue depth for all queues."""

    help = "Print queues and depths"

    def handle(self, **options):
        """Runs [snoop.data.admin.get_stats][] and prints result.
        """

        logging_for_management_command(options['verbosity'])

        tasks.import_snoop_tasks()

        collection_list = sorted(collections.get_all().values(), key=lambda x: x.name)
        queue_list = list(set(f.queue for f in tasks.task_map.values() if f.queue))
        random.shuffle(collection_list)
        random.shuffle(queue_list)
        for collection in collection_list:
            log.info(f'{"=" * 10} collection "{collection.name}" {"=" * 10}')
            for q in queue_list:
                if q:
                    q = collection.queue_name + '.' + q
                    log.info('queue "%s": depth = %s', q, tasks.get_rabbitmq_queue_length(q))
