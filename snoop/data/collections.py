"""Manage data into separate collections (and corresponding indexes, databases, and object storages) under
one Django instance.

Each Collection is bound to one PostgreSQL database, one Elasticsearch index, and one object storage
location (known as "blob directory"). This module defines operations to create and remove each of these, as
well as to list every resource on this server.


When writing any data-oriented code, a Collection must be selected (in order to know the correct database,
index and object storage to use). This is done through the context manager `Collection.set_current()`.
Inside a collection context, `collection.current()` will return the collection set in the context manager,
and any `snoop.data.models.MultiDBModel` can be used with Django ORM and will use that collection's
database.

Internally, this is stored in `threading.local` memory on entering the context manager, and fetched from
that same local memory whenever required inside the context. This means we can do multi-threaded work on
different collections at different points in time, from the same process. This also means we sometimes have
to patch Django's different admin, database and framework classes to either read or write to our current
collection storage.

The list of collections is static and supplied through a single dict in `settings.SNOOP_COLLECTIONS`.
This means a Django server restart is required whenever the collection count or configuration is changed.

This module creates Collection instances from the setting and stores them in a global called `ALL`. This
global is usually used in management commands to select the collection requested by the user.
"""


import logging
import subprocess
import threading
from contextlib import contextmanager
from pathlib import Path

from django.conf import settings
from django.db import connection
from django.core import management
from django.db import transaction

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ALL = {}
ALL_TESSERACT_LANGS = subprocess.check_output(
    "tesseract --list-langs | tail -n +2",
    shell=True).decode().split()
threadlocal = threading.local()


class Collection:
    """Model for managing collection resources: SQL databases, ES indexes, object storage.

    Accepts additional settings for switching off processing, switching on periodic sync of the dataset, OCR
    language list, index-level settings.

    The collection name is restricted to a very simple format and used directly to obtain: a PG database
    name, an ES index name, and two folders on disk: one directly under the DATA_DIR, with the initial
    dataset, and another one directly under the BLOB_DIR, used to store all the binary data for
    `snoop.data.models.Blob` objects. All these names and paths are retrieved as properties on the
    Collection object.
    """

    DATA_DIR = 'data'
    GPGHOME_DIR = 'gpghome'

    def __init__(self, name, process=False, sync=False, **opt):
        self.name = name
        self.process = process
        self.sync = sync and process
        self.ocr_languages = opt.get('ocr_languages', [])
        self.max_result_window = opt.get('max_result_window', 10000)
        self.refresh_interval = opt.get('refresh_interval', "5s")

        for lang_grp in self.ocr_languages:
            assert lang_grp.strip() != ''
            for lang in lang_grp.split('+'):
                assert lang in ALL_TESSERACT_LANGS, \
                    f'language code "{lang}" is not available'

    def __repr__(self):
        return f"<Collection {self.name} process={self.process} sync={self.sync}>"

    @property
    def db_name(self):
        return f"collection_{self.name}"

    @property
    def db_alias(self):
        return f"collection_{self.name}"

    @property
    def queue_name(self):
        return f"collection_{self.name}"

    @property
    def base_path(self):
        if settings.SNOOP_COLLECTION_ROOT is None:
            raise RuntimeError("settings.SNOOP_COLLECTION_ROOT not configured")
        return Path(settings.SNOOP_COLLECTION_ROOT) / self.name

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
            # this causes some tests with rollbacks to fail
            # if old is None:
            #     close_old_connections()


def all_collection_dbs():
    with connection.cursor() as conn:
        conn.execute('SELECT datname FROM pg_database WHERE datistemplate = false')
        return [name for (name,) in conn.fetchall() if name.startswith('collection_')]


def drop_db(db_name):
    with connection.cursor() as conn:
        conn.execute(f'DROP DATABASE "{db_name}"')


def create_databases():
    dbs = all_collection_dbs()
    for col in ALL.values():
        if col.db_name not in dbs:
            logger.info(f'Creating database {col.db_name}')
            with connection.cursor() as conn:
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
            indexing.update_mapping()


def create_roots():
    """Creates a root directory (bucket) for the collection in the blob directory.

    Also creates a root document entry for the input data, so we have something to export.
    """

    from .models import blob_root, Directory

    for col in ALL.values():
        with transaction.atomic(using=col.db_alias), col.set_current():
            root_path = blob_root()
            # Avoid to run mkdir over a symlink.
            # This will still error out if there's a file at that location.
            if not root_path.is_symlink():
                root_path.mkdir(exist_ok=True, parents=True)

            root = Directory.root()
            if not root:
                root = Directory.objects.create()
                logger.debug(f'Creating root document {root} for collection {col.name}')


class CollectionsRouter:
    """Django database router.

    Uses the current collection's `.db_alias` to decide what database to route the reads and writes to.
    """

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
    """Get back the Collection that was set in the `Collections.set_current()` context manager.

    Raises if not called from inside the `Collections.set_current()` context.
    """
    col = getattr(threadlocal, 'collection', None)
    assert col is not None, "There is no current collection set"
    return col


for item in settings.SNOOP_COLLECTIONS:
    col = Collection(**item)
    ALL[col.name] = col
