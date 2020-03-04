import logging
import threading
from contextlib import contextmanager
from pathlib import Path

from django.conf import settings
from django.db import connection
from django.core import management

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

threadlocal = threading.local()


class Collection:

    DATA_DIR = 'data'
    GPGHOME_DIR = 'gpghome'

    def __init__(self, name, process=False, sync=False):
        self.name = name
        self.process = process or sync
        self.sync = sync

    def __repr__(self):
        return f"<Collection {self.name} process={self.process} sync={self.sync}>"

    @property
    def db_name(self):
        return f"collection_{self.name}"

    @property
    def db_alias(self):
        return f"collection_{self.name}"

    @property
    def base_path(self):
        if settings.SNOOP_COLLECTION_ROOT is None:
            raise RuntimeError("settings.SNOOP_COLLECTION_ROOT not configured")
        return Path(settings.SNOOP_COLLECTION_ROOT) / col.name

    @property
    def data_path(self):
        return self.base_path / self.DATA_DIR

    @property
    def gpghome_path(self):
        return self.base_path / self.GPGHOME_DIR

    @property
    def es_index(self):
        return self.name

    def migrate(self):
        management.call_command('migrate', '--database', self.db_alias)

    @contextmanager
    def set_current(self):
        old = getattr(threadlocal, 'collection', None)
        assert old in (None, self), \
            "There is already a current collection"
        try:
            threadlocal.collection = self
            logger.debug("WITH collection = %s BEGIN", self)
            yield
        finally:
            logger.debug("WITH collectio = %s END", self)
            assert threadlocal.collection is self, \
                "Current collection has changed!"
            threadlocal.collection = old


ALL = {}
for item in settings.SNOOP_COLLECTIONS:
    col = Collection(**item)
    ALL[col.name] = col


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


def create_es_indexes():
    from snoop.data import indexing
    for col in ALL.values():
        with col.set_current():
            if not indexing.index_exists():
                logger.info(f'Creating index {col.es_index}')
                indexing.create_index()


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
