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

    @contextmanager
    def set_current(self):
        assert getattr(threadlocal, 'collection', None) is None, \
            "There is already a current collection"
        try:
            threadlocal.collection = self
            logger.debug("WITH collection = %s BEGIN", self)
            yield
        finally:
            logger.debug("WITH collectio = %s END", self)
            assert threadlocal.collection is self, \
                "Current collection has changed!"
            threadlocal.collection = None


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

    def db_for_read(self, model, instance=None, **hints):
        if model._meta.app_label in self.snoop_app_labels:
            if instance is None:
                db_alias = current().db_alias
            else:
                db_alias = from_object(instance).db_alias
            logger.debug("Sending to db %r", db_alias)
            return db_alias

    def db_for_write(self, model, **hints):
        return self.db_for_read(model, **hints)

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """
        Snoop models not allowed in 'default'; other models not allowed
        in collection_* databases
        """
        if db == 'default':
            return (app_label not in self.snoop_app_labels)
        else:
            return (app_label in self.snoop_app_labels)


def from_object(obj):
    db_alias = obj._state.db
    assert db_alias.startswith('collection_')
    return ALL[db_alias.split('_', 1)[1]]


def current():
    col = getattr(threadlocal, 'collection', None)
    assert col is not None, "There is no current collection set"
    return col
