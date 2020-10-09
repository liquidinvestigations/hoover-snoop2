import os

from snoop.defaultsettings import *

SNOOP_COLLECTIONS_ELASTICSEARCH_URL = os.environ.get('SNOOP_ES_URL', 'http://localhost:9200')
SNOOP_TESTDATA = SNOOP_COLLECTION_ROOT + '/testdata'
assert os.path.isdir(SNOOP_TESTDATA)

CELERY_TASK_ALWAYS_EAGER = True
CELERY_EAGER_PROPAGATES_EXCEPTIONS = True
