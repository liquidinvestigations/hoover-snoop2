import os

from snoop.defaultsettings import *

SNOOP_TESTDATA = SNOOP_COLLECTION_ROOT + '/testdata'
print(SNOOP_TESTDATA)
print(os.listdir(SNOOP_TESTDATA))
assert os.path.isdir(SNOOP_TESTDATA)

CELERY_TASK_ALWAYS_EAGER = True
CELERY_EAGER_PROPAGATES_EXCEPTIONS = True
