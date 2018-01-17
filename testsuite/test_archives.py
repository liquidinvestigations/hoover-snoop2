import json
import pytest
from pathlib import Path
from django.conf import settings
from snoop.data import models
from snoop.data.analyzers import archives

pytestmark = [pytest.mark.django_db]

def test_unarchive_zip():
    JERRY_ZIP = Path(settings.SNOOP_TESTDATA) / "data/disk-files/archives/tom/jail/jerry.zip"

    zip_blob = models.Blob.create_from_file(JERRY_ZIP)
    listing_blob = archives.unarchive(zip_blob)
    with listing_blob.open() as f:
        listing = json.load(f)

    STOCK_PHOTO = {
        'blob_pk': 'f7281c8a9cc404816f019382bd121c5fff28e0b816f632f43069b1f7c40c3e6e',
        'name': 'stock-photo-house-mouse-standing-on-rear-feet-mus-musculus-137911070.jpg',
        'type': 'file',
    }
    IS_THIS = {
        'blob_pk': '2228e662341d939650d313b8971984d99b0d50791f7b4c06034b6f254436a3c3',
        'name': 'is this?',
        'type': 'file',
    }
    PACKAGE_JSON = {
        'blob_pk': 'de16d79543b6aeaf19c025264cf9a368ce6bdd3f7375091835dc40a8559056cd',
        'name': 'package.json',
        'type': 'file',
    }
    JERRY_7Z = {
        'blob_pk': '84e69da35c4fa4c4a3e7be2a1ff30773aaeb107258618d788897da8e634d3ff0',
        'name': 'jerry.7z',
        'type': 'file',
    }
    MOUSE_DIR = {
        'children': [STOCK_PHOTO],
        'name': 'mouse',
        'type': 'directory',
    }
    WHAT_DIR = {
        'children': [IS_THIS],
        'name': 'what',
        'type': 'directory',
    }
    ETC_DIR = {
        'children': [JERRY_7Z],
        'name': 'etc',
        'type': 'directory',
    }
    JERRY_DIR = {
        'children': [
            PACKAGE_JSON,
        ],
        'name': 'jerry',
        'type': 'directory',
    }

    assert listing[0]['name'] == 'jerry'
    assert listing[0]['type'] == 'directory'
    assert ETC_DIR in listing[0]['children']
    assert PACKAGE_JSON in listing[0]['children']
    assert WHAT_DIR in listing[0]['children']
    assert MOUSE_DIR in listing[0]['children']
