import os

from snoop.defaultsettings import *

SNOOP_BLOB_STORAGE = str(base_dir / 'test_blobs')

default_testdata_path = str(base_dir.parent / 'collections' / 'testdata')
SNOOP_TESTDATA = os.getenv('SNOOP_TESTDATA', default_testdata_path)
SNOOP_COLLECTION_ROOT = str(Path(SNOOP_TESTDATA) / 'data')

assert os.path.isdir(SNOOP_TESTDATA)

CELERY_TASK_ALWAYS_EAGER = True
CELERY_EAGER_PROPAGATES_EXCEPTIONS = True
