import os

from snoop.defaultsettings import *

SNOOP_TESTDATA = '/opt/hoover/collections/testdata'
assert os.path.isdir(SNOOP_TESTDATA)

CELERY_TASK_ALWAYS_EAGER = True
CELERY_EAGER_PROPAGATES_EXCEPTIONS = True
ALWAYS_QUEUE_NOW = True

# NLP_TEXT_LENGTH_LIMIT = 15000
TRANSLATION_TEXT_LENGTH_LIMIT = 100

# set connection and statement timeouts for database
CONNECT_TIMEOUT = 300  # s
STATEMENT_TIMEOUT = 300_000  # ms
DB_OPTIONS = {'connect_timeout': CONNECT_TIMEOUT, "options": f"-c statement_timeout={STATEMENT_TIMEOUT}ms"}
DATABASES['collection_testdata']['OPTIONS'] = DB_OPTIONS
DATABASES['default']['OPTIONS'] = DB_OPTIONS

# save original values for optional disabling
ORIG_SNOOP_THUMBNAIL_URL = SNOOP_THUMBNAIL_URL
ORIG_SNOOP_OBJECT_DETECTION_URL = SNOOP_OBJECT_DETECTION_URL
ORIG_EXTRACT_ENTITIES = EXTRACT_ENTITIES
ORIG_DETECT_LANGUAGE = DETECT_LANGUAGE
ORIG_TRANSLATION_URL = TRANSLATION_URL
ORIG_OCR_ENABLED = OCR_ENABLED

# nullify originals to remove optionals for all tests where they're not activated
SNOOP_THUMBNAIL_URL = None
SNOOP_OBJECT_DETECTION_URL = None
EXTRACT_ENTITIES = False
DETECT_LANGUAGE = False
TRANSLATION_URL = None
OCR_ENABLED = False
