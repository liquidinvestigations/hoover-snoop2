import os
from urllib.parse import urlparse

from .defaultsettings import *  # noqa: F401

ALLOWED_HOSTS = ['snoop']

snoop_base_url = os.environ['DOCKER_HOOVER_SNOOP_BASE_URL']
if snoop_base_url:
    ALLOWED_HOSTS.append(urlparse(snoop_base_url).netloc)

SECRET_KEY = os.environ['DOCKER_HOOVER_SNOOP_SECRET_KEY']
DEBUG = bool(os.environ.get('DOCKER_HOOVER_SNOOP_DEBUG'))

default_db = {
    'ENGINE': 'django.db.backends.postgresql',
    'NAME': 'snoop',
    'USER': 'snoop',
    'HOST': 'snoop-pg',
    'PORT': 5432,
}

DATABASES = {
    'default': default_db,
    'collection_testdata': dict(default_db, NAME='collection_testdata'),
}

SNOOP_COLLECTIONS = ['testdata']

CELERY_BROKER_URL = 'amqp://snoop-rabbitmq'

SNOOP_TIKA_URL = 'http://snoop-tika:9998'

SNOOP_COLLECTIONS_ELASTICSEARCH_URL = 'http://search-es:9200'
