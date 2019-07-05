import os

from snoop.defaultsettings import *

SNOOP_BLOB_STORAGE = str(base_dir / 'test_blobs')

default_testdata_path = str(base_dir.parent / 'collections' / 'testdata')
SNOOP_TESTDATA = os.getenv('SNOOP_TESTDATA', default_testdata_path)
SNOOP_COLLECTION_ROOT = str(Path(SNOOP_TESTDATA) / 'data')

assert os.path.isdir(SNOOP_TESTDATA)

CELERY_TASK_ALWAYS_EAGER = True
CELERY_EAGER_PROPAGATES_EXCEPTIONS = True

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'snoop',
        'USER': 'snoop',
        'HOST': 'snoop-pg',
        'PORT': 5432,
    },
}

# CELERY_BROKER_URL = 'amqp://snoop-rabbitmq'

TIKA_URL = 'http://snoop-tika:9998'

SNOOP_COLLECTIONS_ELASTICSEARCH_URL = 'http://search-es:9200'
# SNOOP_STATS_ELASTICSEARCH_URL = 'http://snoop-stats-es:9200'
# SNOOP_STATS_ELASTICSEARCH_INDEX_PREFIX = 'http://snoop-stats-es:9200'
# SNOOP_GNUPG_HOME = '/opt/hoover/gnupg'
