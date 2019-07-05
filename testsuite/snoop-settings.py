import os
from urllib.parse import urlparse

from .defaultsettings import *  # noqa: F401

ALLOWED_HOSTS = ['snoop']

snoop_base_url = os.environ['DOCKER_HOOVER_SNOOP_BASE_URL']
if snoop_base_url:
    ALLOWED_HOSTS.append(urlparse(snoop_base_url).netloc)

SECRET_KEY = os.environ['DOCKER_HOOVER_SNOOP_SECRET_KEY']
DEBUG = bool(os.environ.get('DOCKER_HOOVER_SNOOP_DEBUG'))

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'snoop',
        'USER': 'snoop',
        'HOST': 'snoop-pg',
        'PORT': 5432,
    },
}

CELERY_BROKER_URL = 'amqp://snoop-rabbitmq'

TIKA_URL = 'http://snoop-tika:9998'

if os.environ.get('DOCKER_HOOVER_SNOOP_STATS', 'on') == 'on':
    SNOOP_STATS_ELASTICSEARCH_URL = 'http://snoop-stats-es:9200'

SNOOP_COLLECTIONS_ELASTICSEARCH_URL = 'http://search-es:9200'
