"""Command to re-run a single task.

Supports running the task outside of the task system (in the foreground) for debugging with `pdb`:

    python -m pdb ./manage.py retrytask testdata --fg  666

where `666` is the id of the Task you get from the Admin UI at [snoop.data.admin.TaskAdmin].

"""

from django.core.management.base import BaseCommand
from ...logs import logging_for_management_command
from ... import models, collections
from ...tasks import retry_task, import_snoop_tasks


class Command(BaseCommand):
    """Schedule re-running a single task."""

    help = "Retry running task"

    def add_arguments(self, parser):
        """Arguments - the collection, the task ID, some flags."""

        parser.add_argument('collection', help="collection name")
        parser.add_argument('task_pk', type=str, help="Primary key of a task for a retry.")
        parser.add_argument('--fg', action='store_true', help="Run task in foreground mode.")

    def handle(self, collection, task_pk, **options):
        """Runs [snoop.data.tasks.retry_task][] with given options."""

        logging_for_management_command()
        assert collection in collections.get_all(), 'collection does not exist'
        import_snoop_tasks()
        with collections.get_all()[collection].set_current():
            task = models.Task.objects.get(pk=task_pk)
            retry_task(task, fg=options['fg'])
