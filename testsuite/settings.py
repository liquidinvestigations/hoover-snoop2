import os

from snoop.defaultsettings import *

SNOOP_TESTDATA = '/opt/hoover/collections/testdata'
assert os.path.isdir(SNOOP_TESTDATA)

CELERY_TASK_ALWAYS_EAGER = True
CELERY_EAGER_PROPAGATES_EXCEPTIONS = True
ALWAYS_QUEUE_NOW = True

NLP_TEXT_LENGTH_LIMIT = 25000
TRANSLATION_TEXT_LENGTH_LIMIT = 300
PDF2PDFOCR_MAX_STRLEN = 300 * (2 ** 10)
PDF2PDFOCR_MAX_FILE_LEN = 40 * (2 ** 20)

# set connection and statement timeouts for database
CONNECT_TIMEOUT = 300  # s
STATEMENT_TIMEOUT = 300_000  # ms
DB_OPTIONS = {'connect_timeout': CONNECT_TIMEOUT, "options": f"-c statement_timeout={STATEMENT_TIMEOUT}ms"}
DATABASES['collection_testdata']['OPTIONS'] = DB_OPTIONS
DATABASES['default']['OPTIONS'] = DB_OPTIONS
