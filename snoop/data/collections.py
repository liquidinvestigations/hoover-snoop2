"""Manage data into separate collections (and corresponding indexes, databases, and object storages) under
one Django instance.

Each Collection is bound to one PostgreSQL database, one Elasticsearch index, and one object storage
location (known as "blob directory"). This module defines operations to create and remove each of these, as
well as to list every resource on this server.


When writing any data-oriented code, a Collection must be selected (in order to know the correct database,
index and object storage to use). This is done through the context manager
[`Collection.set_current()`][snoop.data.collections.Collection.set_current]. Inside a collection context,
[`collection.current()`][snoop.data.collections.current] will return the collection set in the context
manager, and any Model can be used with Django ORM and will use that collection's database.

Internally, this is stored in Python's `threading.local` memory on entering the context manager, and fetched
from that same local memory whenever required inside the context. This means we can do multi-threaded work
on different collections at different points in time, from the same process. This also means we sometimes
have to patch Django's different admin, database and framework classes to either read or write to our
current collection storage.

The list of collections is static and supplied through a single dict in
[settings.SNOOP_COLLECTIONS][snoop.defaultsettings.SNOOP_COLLECTIONS]. This means a Django server restart is
required whenever the collection count or configuration is changed.

This module creates Collection instances from the setting and stores them in a global called
[`ALL`][snoop.data.collections.ALL]. This global is usually used in management commands to select the
collection requested by the user.
"""

import os
import io
import logging
import subprocess
import threading
from contextlib import contextmanager

from django.conf import settings
from django.db import connection
from django.core import management
from django.db import transaction

from .s3 import get_mount

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ALL = {}
"""Global dictionary storing all the collections.
"""


ALL_TESSERACT_LANGS = subprocess.check_output(
    "tesseract --list-langs | tail -n +2",
    shell=True).decode().split()
"""Global list with all the available OCR languages.
"""

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
        """Initialize object.

        Raises:
            AssertionError: if OCR language configuration is incorrect.
        """

        self.name = name
        self.process = process
        self.sync = sync and process
        self.ocr_languages = opt.get('ocr_languages', [])
        self.max_result_window = opt.get('max_result_window', 10000)
        self.refresh_interval = opt.get('refresh_interval', "1s")
        self.opt = opt

        for lang_grp in self.ocr_languages:
            assert lang_grp.strip() != ''
            for lang in lang_grp.split('+'):
                assert lang in ALL_TESSERACT_LANGS, \
                    f'language code "{lang}" is not available'

        # parse default table heads: different variante
        table_headers = self.opt.get('default_table_header', '').strip()
        variant_list = [[col.strip() for col in variant.split(':')] for variant in table_headers.split(';')]
        self.default_table_head_by_len = {
            len(variant): variant
            for variant in variant_list
            if len(variant) > 1
        }
        self.explode_table_rows = self.opt.get('explode_table_rows', False)

    def get_default_queues(self):
        """Return a list of queues which should run on the "default" worker
        for this collection. This is required to make sure disabled tasks are not
        left forever in "pending" state.
        """
        lst = []
        if not (self.image_classification_classify_images_enabled
                or self.image_classification_object_detection_enabled):
            lst.append('img-cls')
        if not (self.nlp_language_detection_enabled
                or self.nlp_entity_extraction_enabled):
            lst.append('entities')
        if not self.translation_enabled:
            lst.append('translate')
        if not self.thumbnail_generator_enabled:
            lst.append('thumbnails')
        if not self.pdf_preview_enabled:
            lst.append('pdf-preview')
        lst.append('filesystem')
        lst.append('ocr')
        lst.append('digests')

        return lst

    @property
    def pdf_preview_enabled(self):
        return self.opt.get('pdf_preview_enabled', bool(settings.SNOOP_PDF_PREVIEW_URL)) \
            and bool(settings.SNOOP_PDF_PREVIEW_URL)

    @property
    def thumbnail_generator_enabled(self):
        return self.opt.get(
            'thumbnail_generator_enabled',
            bool(settings.SNOOP_THUMBNAIL_URL)) \
            and bool(settings.SNOOP_THUMBNAIL_URL)

    @property
    def image_classification_object_detection_enabled(self):
        return self.opt.get(
            'image_classification_object_detection_enabled',
            bool(settings.SNOOP_OBJECT_DETECTION_URL)) \
            and bool(settings.SNOOP_OBJECT_DETECTION_URL)

    @property
    def image_classification_classify_images_enabled(self):
        return self.opt.get(
            'image_classification_classify_images_enabled',
            bool(settings.SNOOP_IMAGE_CLASSIFICATION_URL)) \
            and bool(settings.SNOOP_IMAGE_CLASSIFICATION_URL)

    @property
    def nlp_language_detection_enabled(self):
        return self.opt.get(
            'nlp_language_detection_enabled',
            bool(settings.DETECT_LANGUAGE)) \
            and bool(settings.DETECT_LANGUAGE)

    @property
    def nlp_entity_extraction_enabled(self):
        return self.opt.get(
            'nlp_entity_extraction_enabled',
            bool(settings.EXTRACT_ENTITIES)) \
            and bool(settings.EXTRACT_ENTITIES)

    @property
    def nlp_text_length_limit(self):
        return self.opt.get(
            'nlp_text_length_limit',
            settings.NLP_TEXT_LENGTH_LIMIT,
        )

    @property
    def translation_enabled(self):
        return self.opt.get(
            'translation_enabled',
            bool(settings.TRANSLATION_URL)) \
            and bool(settings.TRANSLATION_URL)

    @property
    def translation_target_languages(self):
        return self.opt.get(
            'translation_target_languages',
            '').split(',') \
            or settings.TRANSLATION_TARGET_LANGUAGES

    @property
    def translation_text_length_limit(self):
        return int(
            self.opt.get(
                'translation_text_length_limit',
                settings.TRANSLATION_TEXT_LENGTH_LIMIT,
            )
        )

    def __repr__(self):
        """String representation for a Collection.
        """

        return f"<Collection {self.name} process={self.process} sync={self.sync}>"

    @property
    def db_name(self):
        """Name of SQL database for this collection.
        """
        return f"collection_{self.name}"

    @property
    def db_alias(self):
        """Name of SQL database alias for this collection.

        Identical to `db_name` above.

        TODO:
            investigate merging this property and `db_name`.
        """
        return f"collection_{self.name}"

    @property
    def queue_name(self):
        """Name of message queue for this collection.
        """

        return f"collection_{self.name}"

    @property
    def es_index(self):
        """Name of elasticsearch index for this collection.
        """
        return self.name

    def migrate(self):
        """Run `django migrate` on this collection's database."""

        management.call_command('migrate', '--database', self.db_alias)

    @contextmanager
    def set_current(self):
        """Creates context where this collection is the current one.

        Running this is required to access any of the collection's data from inside database tables.
        """

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

    @contextmanager
    def mount_blobs_root(self, readonly=True):
        """Mount the whole blob root directory under a temporary path, using s3-fuse.

        Another temporary directory is created to store the cache."""

        address = settings.SNOOP_BLOBS_MINIO_ADDRESS
        mount_mode = 'ro' if readonly else 'rw'
        yield get_mount(
            mount_name=f'{self.name}-{mount_mode}-blobs',
            bucket=self.name,
            mount_mode=mount_mode,
            access_key=settings.SNOOP_BLOBS_MINIO_ACCESS_KEY,
            secret_key=settings.SNOOP_BLOBS_MINIO_SECRET_KEY,
            address=address,
        )

    @contextmanager
    def mount_collections_root(self, readonly=True):
        """Mount the whole collections root directory under a temporary path, using s3-fuse.

        Another temporary directory is created to store the cache."""

        address = settings.SNOOP_COLLECTIONS_MINIO_ADDRESS
        mount_mode = 'ro' if readonly else 'rw'

        yield get_mount(
            mount_name=f'{self.name}-{mount_mode}-collections',
            bucket=self.name,
            mount_mode=mount_mode,
            access_key=settings.SNOOP_COLLECTIONS_MINIO_ACCESS_KEY,
            secret_key=settings.SNOOP_COLLECTIONS_MINIO_SECRET_KEY,
            address=address,
        )

    @contextmanager
    def mount_gpghome(self):
        with self.mount_collections_root(readonly=False) as collection_root:
            gpg_root = os.path.join(collection_root, self.GPGHOME_DIR)
            yield gpg_root


def all_collection_dbs():
    """List all the collection databases by asking postgres.
    """
    with connection.cursor() as conn:
        conn.execute('SELECT datname FROM pg_database WHERE datistemplate = false')
        return [name for (name,) in conn.fetchall() if name.startswith('collection_')]


def drop_db(db_name):
    """Run the SQL `DROP DATABASE SQL` command.
    """
    logger.warning('DROPPPING SQL DATABASE %s', db_name)
    with connection.cursor() as conn:
        # https://dba.stackexchange.com/questions/11893/force-drop-db-while-others-may-be-connected/11895#11895
        # stop new connections to db
        conn.execute(f'ALTER DATABASE "{db_name}" CONNECTION LIMIT 0;')
        conn.execute(f"UPDATE pg_database SET datallowconn = 'false' WHERE datname = '{db_name}';")
        # delete existing connections to db
        conn.execute(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{db_name}';")
        # drop database
        conn.execute(f'DROP DATABASE "{db_name}";')
    logger.info('SQL DATABASE %s DROPPED', db_name)


def create_databases():
    """Go through [snoop.data.collections.ALL][] and create databases that don't exist yet."""

    dbs = all_collection_dbs()
    for col in ALL.values():
        if col.db_name not in dbs:
            logger.info(f'Creating database {col.db_name}')
            with connection.cursor() as conn:
                conn.execute(f'CREATE DATABASE "{col.db_name}"')


def migrate_databases():
    """Run database migrations for everything in [snoop.data.collections.ALL][]"""

    for col in ALL.values():
        try:
            logger.info(f'Migrating database {col.db_name}')
            col.migrate()
        except Exception as e:
            logger.exception(e)
            logger.error("Failed to migrate database {col.db_name}")
            raise


def create_es_indexes():
    """Create elasticsearch indexes for everything in [snoop.data.collections.ALL][]"""

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

    from .models import Directory

    for col in ALL.values():
        with transaction.atomic(using=col.db_alias), col.set_current():
            if settings.BLOBS_S3.bucket_exists(col.name):
                logger.info('found bucket %s', col.name)
            else:
                logger.info('creating bucket %s', col.name)
                settings.BLOBS_S3.make_bucket(col.name)
                settings.BLOBS_S3.put_object(col.name, 'tmp/dummy', io.BytesIO(b"hello"), length=5)

            root = Directory.root()
            if not root:
                root = Directory.objects.create()
                logger.info(f'Creating root document {root} for collection {col.name}')


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
    """Get the collection from an instance of an object."""

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
