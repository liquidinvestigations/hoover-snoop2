import os

from snoop.defaultsettings import *

SNOOP_TESTDATA = SNOOP_COLLECTION_ROOT + '/testdata'
assert os.path.isdir(SNOOP_TESTDATA)

CELERY_TASK_ALWAYS_EAGER = True
CELERY_EAGER_PROPAGATES_EXCEPTIONS = True
ALWAYS_QUEUE_NOW = True

# set connection and statement timeouts for database
CONNECT_TIMEOUT = 90  # s
STATEMENT_TIMEOUT = 90_000  # ms
DB_OPTIONS = {'connect_timeout': CONNECT_TIMEOUT, "options": f"-c statement_timeout={STATEMENT_TIMEOUT}ms"}
DATABASES['collection_testdata']['OPTIONS'] = DB_OPTIONS
DATABASES['default']['OPTIONS'] = DB_OPTIONS
