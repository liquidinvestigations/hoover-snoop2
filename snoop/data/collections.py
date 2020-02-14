import logging
import threading
from contextlib import contextmanager

from django.conf import settings
from django.db import connection
from django.core import management

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

threadlocal = threading.local()


class Collection:

    def __init__(self, name):
        self.name = name

    @property
    def db_name(self):
        return f"collection_{self.name}"

    @property
    def db_alias(self):
        return f"collection_{self.name}"

    def migrate(self):
        management.call_command('migrate', '--database', self.db_alias)


ALL = {name: Collection(name) for name in settings.SNOOP_COLLECTIONS}


def create_databases():
    with connection.cursor() as conn:
        conn.execute('SELECT datname FROM pg_database WHERE datistemplate = false')
        dbs = [name for (name,) in conn.fetchall()]
        for col in ALL.values():
            if col.db_name not in dbs:
                logger.info(f'Creating database {col.db_name}')
                conn.execute(f'CREATE DATABASE "{col.db_name}"')


def migrate_databases():
    for col in ALL.values():
        col.migrate()


class CollectionsRouter:

    snoop_app_labels = ['data']

    def db_for_read(self, model, **hints):
        if model._meta.app_label in self.snoop_app_labels:
            db_alias = db()
            assert db_alias is not None
            logger.debug("Sending READ to %s db", db_alias)
            return db_alias

    def db_for_write(self, model, **hints):
        if model._meta.app_label in self.snoop_app_labels:
            db_alias = db()
            assert db_alias is not None
            logger.debug("Sending WRITE to %s db", db_alias)
            return db_alias

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """
        Snoop models not allowed in 'default'; other models not allowed
        in collection_* databases
        """
        if db == 'default':
            return (app_label not in self.snoop_app_labels)
        else:
            return (app_label in self.snoop_app_labels)


@contextmanager
def set_db(db_alias):
    assert getattr(threadlocal, 'db_alias', None) is None
    try:
        threadlocal.db_alias = db_alias
        logger.debug("WITH set_db = %s BEGIN", db_alias)
        yield
    finally:
        logger.debug("WITH set_db = %s END", db_alias)
        assert threadlocal.db_alias == db_alias
        threadlocal.db_alias = None


def db():
    return threadlocal.db_alias
