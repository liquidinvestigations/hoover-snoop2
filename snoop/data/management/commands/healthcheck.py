"""Script used check database connection health.

Not used anymore, since it required a lot of CPU overhead and would prevent the machine from idling with low
CPU usage.

Can still be run once during deployment, or by hand.
"""


from django.core.management.base import BaseCommand
from django.db.migrations.executor import MigrationExecutor
from django.db import connections, DEFAULT_DB_ALIAS


def is_database_synchronized():
    connection = connections[DEFAULT_DB_ALIAS]
    connection.prepare_database()
    executor = MigrationExecutor(connection)
    targets = executor.loader.graph.leaf_nodes()
    return not executor.migration_plan(targets)


class Command(BaseCommand):
    "Check service health: migrations, dependencies"

    def handle(self, **options):
        assert is_database_synchronized(), 'Migrations not run'
        print('database ok')
