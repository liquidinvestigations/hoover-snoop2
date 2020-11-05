import os

from snoop.defaultsettings import *
# m
print(os.listdir("/opt/hoover"))
print(os.listdir("/opt/hoover/collections"))
SNOOP_TESTDATA = SNOOP_COLLECTION_ROOT + '/testdata'
assert os.path.isdir(SNOOP_TESTDATA)

CELERY_TASK_ALWAYS_EAGER = True
CELERY_EAGER_PROPAGATES_EXCEPTIONS = True
