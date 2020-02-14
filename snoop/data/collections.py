import logging

from django.conf import settings
from django.db import connection
from django.core import management

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def create_databases():
    with connection.cursor() as conn:
        conn.execute('SELECT datname FROM pg_database WHERE datistemplate = false')
        dbs = [name for (name,) in conn.fetchall()]
        for name in settings.SNOOP_COLLECTIONS:
            db_name = f'collection_{name}'
            if db_name not in dbs:
                logger.info(f'Creating database {db_name}')
                conn.execute(f'CREATE DATABASE "{db_name}"')


def migrate_databases():
    for name in settings.SNOOP_COLLECTIONS:
        db_name = f'collection_{name}'
        management.call_command('migrate', '--database', db_name)


class CollectionsRouter:

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """
        Snoop models not allowed in 'default'; other models not allowed
        in collection_* databases
        """
        if db == 'default':
            return (app_label != 'data')
        else:
            return (app_label == 'data')
