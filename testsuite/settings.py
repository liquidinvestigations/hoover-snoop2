from snoop.settings import *


DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': str(base_dir / 'db.testing.sqlite3'),
    }
}

SNOOP_BLOB_STORAGE = str(base_dir / 'test_blobs')

testdata_path = base_dir.parent / 'testdata' / 'data'
assert testdata_path.is_dir()

SNOOP_TESTDATA = str(testdata_path)
