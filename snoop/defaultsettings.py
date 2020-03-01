import os
import re
from datetime import timedelta
from pathlib import Path
import json

from snoop.data import celery

base_dir = Path(__file__).resolve().parent.parent

DEBUG = os.environ.get('DEBUG', '').lower() in ['on', 'true']
default_secret_key = 'placeholder key for development'
SECRET_KEY = os.environ.get('SECRET_KEY', default_secret_key)

ALLOWED_HOSTS = []
_hostname = os.environ.get('SNOOP_HOSTNAME')
if _hostname:
    ALLOWED_HOSTS.append(_hostname)

INSTALLED_APPS = [
    'django.contrib.admin',
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

STATIC_URL = '/static/'
STATIC_ROOT = str(base_dir / 'static')

SNOOP_COLLECTIONS_ELASTICSEARCH_URL = os.environ.get('SNOOP_ES_URL', 'http://localhost:9200')

SNOOP_BLOB_STORAGE = str(base_dir / 'blobs')
SNOOP_TIKA_URL = os.environ.get('SNOOP_TIKA_URL', 'http://localhost:9998')
SNOOP_FEED_PAGE_SIZE = 100
SNOOP_COLLECTION_ROOT = os.environ.get('SNOOP_COLLECTION_ROOT')
TASK_PREFIX = os.environ.get('SNOOP_TASK_PREFIX', '')
WORKER_COUNT = int(os.environ.get('SNOOP_WORKER_COUNT', '1'))

# task count to be picked up by 1 worker
WORKER_TASK_LIMIT = 500
# limit for queueing large counts of children tasks
CHILD_QUEUE_LIMIT = 100
# count of pending tasks to trigger when finding an empty queue.
# If there are no pending tasks, this is how many directories
# will be retried by sync every minute.
DISPATCH_QUEUE_LIMIT = 5000


def bool_env(value):
    return (value or '').lower() in ['on', 'true']


SNOOP_DOCUMENT_LOCATIONS_QUERY_LIMIT = 300
SNOOP_DOCUMENT_CHILD_QUERY_LIMIT = 300

_amqp_url = os.getenv('SNOOP_AMQP_URL')
if _amqp_url:
    CELERY_BROKER_URL = _amqp_url

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
        'schedule': timedelta(seconds=30),
    },
}

celery.app.conf.task_routes = {
    'snoop.data.tasks.run_dispatcher': {'queue': 'run_dispatcher'},
}
