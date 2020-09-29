import os
import re
from datetime import timedelta
from pathlib import Path
import json
from multiprocessing import cpu_count

from snoop.data import celery

base_dir = Path(__file__).resolve().parent.parent

DEBUG = os.environ.get('DEBUG', '').lower() in ['on', 'true']
default_secret_key = 'placeholder key for development'
SECRET_KEY = os.environ.get('SECRET_KEY', default_secret_key)

ALLOWED_HOSTS = [os.environ.get('SNOOP_HOSTNAME', '*')]

INSTALLED_APPS = [
    'snoop.data.apps.AdminConfig',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'snoop.data',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',

    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'snoop.data.auto_login.Middleware',

    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'whitenoise.middleware.WhiteNoiseMiddleware',
]

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
}

DATABASES = {
    'default': default_db,
}

SNOOP_COLLECTIONS = json.loads(os.environ.get('SNOOP_COLLECTIONS', '[]'))

for col in SNOOP_COLLECTIONS:
    name = col['name']
    assert re.match(r'^[a-zA-Z0-9-_]+$', name)
    db_name = f'collection_{name}'
    DATABASES[db_name] = dict(default_db, NAME=db_name)

DATABASE_ROUTERS = ['snoop.data.collections.CollectionsRouter']

LANGUAGE_CODE = 'en-us'
DETECT_LANGUAGE = True
LANGUAGE_DETECTOR_NAME = 'polyglot'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_L10N = True
USE_TZ = True

SNOOP_COLLECTIONS_ELASTICSEARCH_URL = os.environ.get('SNOOP_ES_URL', 'http://localhost:9200')

SNOOP_BLOB_STORAGE = str(base_dir / 'blobs')
SNOOP_TIKA_URL = os.environ.get('SNOOP_TIKA_URL', 'http://localhost:9998')
SNOOP_FEED_PAGE_SIZE = 100
SNOOP_COLLECTION_ROOT = os.environ.get('SNOOP_COLLECTION_ROOT')
TASK_PREFIX = os.environ.get('SNOOP_TASK_PREFIX', '')

SNOOP_MIN_WORKERS = int(os.environ.get('SNOOP_MIN_WORKERS', '2'))
SNOOP_MAX_WORKERS = int(os.environ.get('SNOOP_MAX_WORKERS', '8'))
SNOOP_CPU_MULTIPLIER = float(os.environ.get('SNOOP_CPU_MULTIPLIER', '0.85'))
WORKER_COUNT = min(SNOOP_MAX_WORKERS,
                   max(SNOOP_MIN_WORKERS,
                       int(SNOOP_CPU_MULTIPLIER * cpu_count())))

TASK_RETRY_AFTER_DAYS = 35

# max tasks count to be finished by 1 worker before restarting it
WORKER_TASK_LIMIT = 10 ** 5
# memory limit for each worker (in mb),
# not enforced - worker gets restarted after it uses more than this value.
WORKER_MEMORY_LIMIT = 5000

# average worker count to scale the queue limits by
_scale_coef = int((1 + SNOOP_MIN_WORKERS + SNOOP_MAX_WORKERS) / 2)
# limit for queueing large counts of children tasks
CHILD_QUEUE_LIMIT = 50 * _scale_coef
# Count of pending tasks to trigger per collection when finding an empty queue.
# A single worker core running zero-length tasks gets at most around 40
# tasks/s, so to keep them all occupied for 6min:
DISPATCH_QUEUE_LIMIT = 14400 * _scale_coef
# If there are no pending tasks, this is how many directories
# will be retried by sync every minute.
SYNC_RETRY_LIMIT = 60 * _scale_coef

# Only run pdf2pdfocr if pdf text word count less than this value:
PDF2PDFOCR_MAX_WORD_COUNT = 666

# url prefix for all the views, for example "snoop/"
URL_PREFIX = os.getenv('SNOOP_URL_PREFIX', '')
if URL_PREFIX:
    assert URL_PREFIX.endswith('/') and not URL_PREFIX.startswith('/')

STATIC_URL = '/' + URL_PREFIX + 'static/'
STATIC_ROOT = str(base_dir / 'static')


def bool_env(value):
    return (value or '').lower() in ['on', 'true']


SNOOP_DOCUMENT_LOCATIONS_QUERY_LIMIT = 300
SNOOP_DOCUMENT_CHILD_QUERY_LIMIT = 300

_amqp_url = os.getenv('SNOOP_AMQP_URL')
if _amqp_url:
    CELERY_BROKER_URL = _amqp_url

# Of the form "1.2.3.4:1234/_path/" (no "http://" prefix).
# Used to query queue lengths. Assumes user/password guest/guest.
SNOOP_RABBITMQ_HTTP_URL = os.getenv('SNOOP_RABBITMQ_HTTP_URL')

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
        'schedule': timedelta(seconds=55),
    },
    'save_stats': {
        'task': 'snoop.data.tasks.save_stats',
        'schedule': timedelta(seconds=66),
    },
}

celery.app.conf.task_routes = {
    'snoop.data.tasks.run_dispatcher': {'queue': 'run_dispatcher'},
    'snoop.data.tasks.save_stats': {'queue': 'save_stats'},
}

SYSTEM_QUEUES = ['run_dispatcher', 'save_stats']
