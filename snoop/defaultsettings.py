"""Default settings file.

This file gets imported both on the Docker image and on the testing configuration.
"""

import os
import re
from datetime import timedelta
from pathlib import Path
import json
from multiprocessing import cpu_count

from snoop.data import celery

# WARNING: Docstrings are placed after the assignment.
# See the example here:
# https://mkdocstrings.github.io/troubleshooting/#my-docstrings-in-comments-are-not-picked-up
# which is probably based off the rejected PEP here: https://www.python.org/dev/peps/pep-0224/
base_dir = Path(__file__).resolve().parent.parent
"""Helper pointing to root dir of repository."""

DEBUG = os.environ.get('DEBUG', '').lower() in ['on', 'true']
"""Enable debug logging.

Loaded from environment variabe with same name.
"""

SECRET_KEY = os.environ.get('SECRET_KEY', 'placeholder')
"""Django secret key.

Loaded from environment variabe with same name.
"""

SILENCED_SYSTEM_CHECKS = ['urls.W002']
"""Used to disable Django warnings."""

ALLOWED_HOSTS = [os.environ.get('SNOOP_HOSTNAME', '*')]
"""List of domains to allow requests for.

Loaded from environment variable `SNOOP_HOSTNAME`, default is `*` (no restrictions).
"""

INSTALLED_APPS = [
    'snoop.data.apps.AdminConfig',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'rest_framework',
    'drf_yasg',
    'snoop.data',
    'graphene_django'
]
"""List of Django apps to load."""

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    # 'django.middleware.csrf.CsrfViewMiddleware',
    'snoop.data.middleware.DisableCSRF',

    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'snoop.data.middleware.AutoLogin',

    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'whitenoise.middleware.WhiteNoiseMiddleware',
]
"""List of Django middleware to load."""

REST_FRAMEWORK = {
    # Use Django's standard `django.contrib.auth` permissions,
    # or allow read-only access for unauthenticated users.
    'DEFAULT_PERMISSION_CLASSES': [
        # 'rest_framework.permissions.DjangoModelPermissionsOrAnonReadOnly'
        'rest_framework.permissions.AllowAny'
    ],
    'DEFAULT_AUTHENTICATION_CLASSES': [
        'rest_framework.authentication.SessionAuthentication',
    ],
    'DEFAULT_PARSER_CLASSES': [
        'rest_framework.parsers.JSONParser',
    ],
    'DEFAULT_RENDERER_CLASSES': [
        'rest_framework.renderers.JSONRenderer',
        #    'rest_framework.renderers.BrowsableAPIRenderer',
        #    'rest_framework.renderers.AdminRenderer',
    ],
}
"""Configuration for Django Rest Framework.

Disables authentication, allows all access. Sets JSON as the default input and output.
"""

ROOT_URLCONF = 'snoop.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'snoop.wsgi.application'
"""Configure which WSGI application to use, for Django."""

# heroku-style db config
_snoop_db = os.environ['SNOOP_DB']
dbm = re.match(
    r'postgresql://(?P<user>[^:]+):(?P<password>[^@]+)'
    r'@(?P<host>[^:]+):(?P<port>\d+)/(?P<name>.+)',
    _snoop_db,
)
if not dbm:
    raise RuntimeError("Can't parse SNOOP_DB value %r" % _snoop_db)

default_db = {
    'ENGINE': 'django.db.backends.postgresql',
    'HOST': dbm.group('host'),
    'PORT': dbm.group('port'),
    'NAME': dbm.group('name'),
    'USER': dbm.group('user'),
    'PASSWORD': dbm.group('password'),
    'CONN_MAX_AGE': 0,
}

DATABASES = {
    'default': default_db,
}
"""Django databases configuration.

Gets populated from the [`SNOOP_COLLECTIONS`](./#snoop.defaultsettings.SNOOP_COLLECTIONS) constant at import
time.
"""

SNOOP_COLLECTIONS = json.loads(os.environ.get('SNOOP_COLLECTIONS', '[]'))
"""Static configuration for the collections list and settings.

Provided througn environment variable at server boot time.

The DATABASES is expanded with the databases for all these collections here.
"""

for col in SNOOP_COLLECTIONS:
    name = col['name']
    assert re.match(r'^[a-zA-Z0-9-_]+$', name)
    db_name = f'collection_{name}'
    DATABASES[db_name] = dict(default_db, NAME=db_name)

DATABASE_ROUTERS = ['snoop.data.collections.CollectionsRouter']
"""Activate our database router under [snoop.data.collections.CollectionsRouter][].
"""

CELERY_DB_REUSE_MAX = 0
"""Instruct Celery to not reuse database connections.
"""

LANGUAGE_CODE = 'en-us'
"""Django locale."""


DETECT_LANGUAGE = os.getenv('SNOOP_DETECT_LANGUAGES', 'False').lower() == 'true'
EXTRACT_ENTITIES = os.getenv('SNOOP_EXTRACT_ENTITIES', 'False').lower() == 'true'
SNOOP_NLP_URL = os.environ.get('SNOOP_NLP_URL', 'http://127.0.0.1:5000/')
""" URL pointing to NLP server"""

TRANSLATION_URL = os.getenv('SNOOP_TRANSLATION_URL')
TRANSLATION_TEXT_LENGTH_LIMIT = int(os.getenv('SNOOP_TRANSLATION_TEXT_LENGTH_LIMIT', '400'))
TRANSLATION_TARGET_LANGUAGES = os.getenv('SNOOP_TRANSLATION_TARGET_LANGUAGES', "en,de").split(',')
if TRANSLATION_URL:
    assert len(TRANSLATION_TARGET_LANGUAGES) > 0

TIME_ZONE = 'UTC'
USE_I18N = True
USE_L10N = True
USE_TZ = True

SNOOP_COLLECTIONS_ELASTICSEARCH_URL = os.environ.get('SNOOP_ES_URL', 'http://localhost:9200')
"""URL pointing to Elasticsearch server."""

SNOOP_TEMP_STORAGE = str(base_dir / 'tmp')
"""Full disk path pointing to temp storage.
"""


SNOOP_BLOBS_MINIO_ADDRESS = os.environ.get('SNOOP_BLOBS_MINIO_ADDRESS', 'http://minio-blobs:9000')
SNOOP_BLOBS_MINIO_ACCESS_KEY = os.environ.get('SNOOP_BLOBS_MINIO_ACCESS_KEY', 'minioadmin')
SNOOP_BLOBS_MINIO_SECRET_KEY = os.environ.get('SNOOP_BLOBS_MINIO_SECRET_KEY', 'minioadmin')
# BLOBS_S3FS = s3fs.S3FileSystem(
#     key=SNOOP_BLOBS_MINIO_ACCESS_KEY,
#     secret=SNOOP_BLOBS_MINIO_ACCESS_KEY,
#     client_kwargs={"endpoint_url": SNOOP_BLOBS_MINIO_ADDRESS},
#     config_kwargs={'signature_version': 's3v4'},
#     use_ssl=False,
#     anon=False,
# )

print('MINIO ADDRESS', SNOOP_BLOBS_MINIO_ADDRESS)
print('MINIO KEY', SNOOP_BLOBS_MINIO_ACCESS_KEY)
print('MINIO SECRET', SNOOP_BLOBS_MINIO_SECRET_KEY)


SNOOP_TIKA_URL = os.environ.get('SNOOP_TIKA_URL', 'http://localhost:9998')
"""URL pointing to Apache Tika server."""

SNOOP_THUMBNAIL_URL = os.environ.get('SNOOP_THUMBNAIL_URL')
SNOOP_PDF_PREVIEW_URL = os.environ.get('SNOOP_PDF_PREVIEW_URL')

SNOOP_IMAGE_CLASSIFICATION_URL = os.environ.get('SNOOP_IMAGE_CLASSIFICATION_URL')
SNOOP_OBJECT_DETECTION_URL = os.environ.get('SNOOP_OBJECT_DETECTION_URL')

SNOOP_FEED_PAGE_SIZE = 100
"""Pagination size for the /feed URLs.

TODO:
    remove this value, as the API is not used anymore.
"""

SNOOP_COLLECTION_ROOT = os.environ.get('SNOOP_COLLECTION_ROOT')
"""Path on disk pointing to collection source directory.

All collections in the system must have a directory here called the same as the collection name, containing
a directory called `data` where the actual collection data is fetched from.
"""

TASK_PREFIX = os.environ.get('SNOOP_TASK_PREFIX', '')
"""Prefix to add to all snoop task queues.

TODO:
    Remove this value, as it's not used anymore.
"""


SNOOP_MIN_WORKERS = int(os.environ.get('SNOOP_MIN_WORKERS', '2'))
"""Input min worker count."""


SNOOP_MAX_WORKERS = int(os.environ.get('SNOOP_MAX_WORKERS', '8'))
"""Input max worker count."""

SNOOP_CPU_MULTIPLIER = float(os.environ.get('SNOOP_CPU_MULTIPLIER', '0.66'))
"""Input CPU multiplier."""


WORKER_COUNT = min(SNOOP_MAX_WORKERS,
                   max(SNOOP_MIN_WORKERS,
                       int(SNOOP_CPU_MULTIPLIER * cpu_count())))
"""Computed worker count for this node."""

TASK_RETRY_AFTER_MINUTES = 3
"""Errored tasks are retried at most every this number of minutes."""

TASK_RETRY_FAIL_LIMIT = 3
"""Errored tasks are retried at most this number of times."""

WORKER_TASK_LIMIT = 50000
"""Max tasks count to be finished by 1 worker process before restarting it.

Used to avoid memory leaks.
"""

WORKER_MEMORY_LIMIT = 500
"""Memory limit for each worker (in mb),

Not enforced during job -- worker gets restarted after it uses more than this value. Used to avoid memory
leaks.
"""

_scale_coef = int((1 + SNOOP_MIN_WORKERS + SNOOP_MAX_WORKERS + WORKER_COUNT) / 3)
""" average worker count to scale the queue limits by
"""

CHILD_QUEUE_LIMIT = 50 * _scale_coef
""" Limit for queueing large counts of children tasks.
"""

DISPATCH_QUEUE_LIMIT = 14400 * _scale_coef
""" Count of pending tasks to trigger per collection when finding an empty queue.

A single worker core running zero-length tasks gets at most around 40
tasks/s, so to keep them all occupied for 6min: 14400
"""

SYNC_RETRY_LIMIT_DIRS = 60 * _scale_coef
""" If there are no pending tasks, this is how many directories
will be retried by sync every minute.
"""

RETRY_LIMIT_TASKS = 8000 * _scale_coef
"""Number BROKEN/ERROR tasks to retry every minute, while their fail count has not reached the limit.

See `TASK_RETRY_FAIL_LIMIT`."""

PDF2PDFOCR_MAX_STRLEN = 2 * (2 ** 20)
""" Only run pdf2pdfocr if pdf text length less than this value.

This should defend us from over-1000-page documents that hang up the processing for days. The english bible
has about 4 MB of text, so we use 50% of that as a simple value of when to stop.
"""

URL_PREFIX = os.getenv('SNOOP_URL_PREFIX', '')
"""Configuration to set the URL prefix for all service routes. For example: "snoop/".
"""
if URL_PREFIX:
    assert URL_PREFIX.endswith('/') and not URL_PREFIX.startswith('/')

STATIC_URL = '/' + URL_PREFIX + 'static/'
"""Url path pointing to static files, for Django."""


STATIC_ROOT = str(base_dir / 'static')
"""Full disk path to static directory on disk, for Django."""


def bool_env(value):
    return (value or '').lower() in ['on', 'true']


SNOOP_DOCUMENT_LOCATIONS_QUERY_LIMIT = 200
"""Limit page size when listing document locations.
"""

SNOOP_DOCUMENT_CHILD_QUERY_LIMIT = 200
"""Limit page size when listing directory children.
"""

_amqp_url = os.getenv('SNOOP_AMQP_URL')
if _amqp_url:
    CELERY_BROKER_URL = _amqp_url
CELERYD_HIJACK_ROOT_LOGGER = False

SNOOP_RABBITMQ_HTTP_URL = os.getenv('SNOOP_RABBITMQ_HTTP_URL')
"""URL pointing to RabbitMQ message queue.

Of the form "1.2.3.4:1234/_path/" (no "http://" prefix).
Used to query queue lengths.

Username and password configs follow.
"""

SNOOP_RABBITMQ_HTTP_USERNAME = os.getenv('SNOOP_RABBITMQ_HTTP_USERNAME', 'guest')
"""Username for rabbitmq HTTP interface. Default 'guest' """

SNOOP_RABBITMQ_HTTP_PASSWORD = os.getenv('SNOOP_RABBITMQ_HTTP_PASSWORD', 'guest')
"""Password for rabbitmq HTTP interface. Default 'guest' """

_tracing_url = os.environ.get('TRACING_URL')
if _tracing_url:
    trm = re.match(r'http://(?P<host>[^:]+):(?P<port>\d+)', _tracing_url)
    if not trm:
        raise RuntimeError("Can't parse TRACING_API value %r" % _tracing_url)

    TRACING_ENABLED = True
    TRACING_HOST = trm.group('host')
    TRACING_PORT = int(trm.group('port'))
    TRACING_API = '/api/v2/spans'

celery.app.conf.beat_schedule = {
    'run_dispatcher': {
        'task': 'snoop.data.tasks.run_dispatcher',
        'schedule': timedelta(seconds=54),
    },
    'save_stats': {
        'task': 'snoop.data.tasks.save_stats',
        'schedule': timedelta(seconds=57),
    },
    'update_all_tags': {
        'task': 'snoop.data.tasks.update_all_tags',
        'schedule': timedelta(seconds=35),
    },
    'run_bulk_tasks': {
        'task': 'snoop.data.tasks.run_bulk_tasks',
        'schedule': timedelta(seconds=66),
    },
}

celery.app.conf.task_routes = {
    'snoop.data.tasks.run_dispatcher': {'queue': 'run_dispatcher'},
    'snoop.data.tasks.save_stats': {'queue': 'save_stats'},
    'snoop.data.tasks.update_all_tags': {'queue': 'update_all_tags'},
    'snoop.data.tasks.run_bulk_tasks': {'queue': 'run_bulk_tasks'},
}

SYSTEM_QUEUES = ['run_dispatcher', 'save_stats', 'update_all_tags', 'run_bulk_tasks']
"""List of "system queues" - celery that must be executed periodically.

One execution of any of these functions will work on all collections under a `for` loop.
"""

ALWAYS_QUEUE_NOW = False
"""Setting this to True disables the Task queueing system and executes Task functions in the foregrond. Used
for testing.
"""

if not DEBUG:
    # don't connect to the internet to verify my schema pls
    SWAGGER_SETTINGS = {
        'VALIDATOR_URL': None,
    }

GRAPHENE = {
    'SCHEMA': 'snoop.data.schema.schema'
}
