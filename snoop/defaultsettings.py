import os
import re
from datetime import timedelta
from pathlib import Path

from snoop.data import celery

base_dir = Path(__file__).resolve().parent.parent

DEBUG = os.environ.get('DEBUG', '').lower() in ['on', 'true']
_default_secret_key = 'placeholder key for development'
SECRET_KEY = os.environ.get('SECRET_KEY', _default_secret_key)

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

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'snoop2',
    }
}

# heroku-style db config
_snoop_db = os.environ.get('SNOOP_DB')
if _snoop_db:
    dbm = re.match(
        r'postgresql://(?P<user>[^:]+):(?P<password>[^@]+)'
        r'@(?P<host>[^:]+):(?P<port>\d+)/(?P<name>.+)',
        _snoop_db,
    )
    if not dbm:
        raise RuntimeError("Can't parse SNOOP_DB value %r" % _snoop_db)
    DATABASES['default']['HOST'] = dbm.group('host')
    DATABASES['default']['PORT'] = dbm.group('port')
    DATABASES['default']['NAME'] = dbm.group('name')
    DATABASES['default']['USER'] = dbm.group('user')
    DATABASES['default']['PASSWORD'] = dbm.group('password')

LANGUAGE_CODE = 'en-us'
DETECT_LANGUAGE = True
TIME_ZONE = 'UTC'
USE_I18N = True
USE_L10N = True
USE_TZ = True

STATIC_URL = '/static/'
STATIC_ROOT = str(base_dir / 'static')

SNOOP_COLLECTION_NAME = 'snoop'
SNOOP_COLLECTIONS_ELASTICSEARCH_URL = os.environ.get('SNOOP_ES_URL', 'http://localhost:9200')

SNOOP_BLOB_STORAGE = str(base_dir / 'blobs')
SNOOP_TIKA_URL = os.environ.get('SNOOP_TIKA_URL', 'http://localhost:9998')
SNOOP_GNUPG_HOME = None
SNOOP_FEED_PAGE_SIZE = 100
SNOOP_COLLECTIONS_ELASTICSEARCH_INDEX = os.environ.get('SNOOP_ES_INDEX', 'snoop2')
SNOOP_COLLECTION_ROOT = os.environ.get('SNOOP_COLLECTION_ROOT')
SNOOP_STATS_ELASTICSEARCH_URL = None
SNOOP_STATS_ELASTICSEARCH_INDEX_PREFIX = 'snoop2-'
TASK_PREFIX = os.environ.get('SNOOP_TASK_PREFIX', '')

_amqp_url = os.environ.get('SNOOP_AMQP_URL')
if _amqp_url:
    CELERY_BROKER_URL = _amqp_url

celery.app.conf.beat_schedule = {
    'check_if_idle': {
        'task': 'snoop.data.tasks.check_if_idle',
        'schedule': timedelta(seconds=60),
    }
}

celery.app.conf.task_routes = {
    'snoop.data.tasks.check_if_idle': {'queue': 'watchdog'}
}
