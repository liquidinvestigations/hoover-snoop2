from snoop.defaultsettings import *
import os

SNOOP_BLOB_STORAGE = str(base_dir / 'test_blobs')

default_testdata_path = str(base_dir.parent / 'testdata')
SNOOP_TESTDATA = os.getenv('SNOOP_TESTDATA', default_testdata_path)

assert os.path.isdir(SNOOP_TESTDATA)

CELERY_TASK_ALWAYS_EAGER = True
