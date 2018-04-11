import json
import pytest
from pathlib import Path
from django.conf import settings
from snoop.data import models, filesystem
from snoop.data.analyzers import archives
from snoop.data.utils import time_from_unix

pytestmark = [pytest.mark.django_db]

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

JERRY_ZIP = Path(settings.SNOOP_TESTDATA) / "data/disk-files/archives/tom/jail/jerry.zip"
ZIP_DOCX = Path(settings.SNOOP_TESTDATA) / "data/disk-files/archives/zip-with-docx-and-doc.zip"
JANE_DOE_PST = Path(settings.SNOOP_TESTDATA) / "data/pst/flags_jane_doe.pst"
SHAPELIB_MBOX = Path(settings.SNOOP_TESTDATA) / "data/mbox/shapelib.mbox"
TAR_GZ = Path(settings.SNOOP_TESTDATA) / "data/disk-files/archives/targz-with-pdf-doc-docx.tar.gz"
RAR = Path(settings.SNOOP_TESTDATA) / "data/disk-files/archives/rar-with-pdf-doc-docx.rar"

def test_unarchive_zip(taskmanager):
    zip_blob = models.Blob.create_from_file(JERRY_ZIP)
    listing_blob = archives.unarchive(zip_blob)
    with listing_blob.open() as f:
        listing = json.load(f)

    assert listing[0]['name'] == 'jerry'
    assert listing[0]['type'] == 'directory'
    assert ETC_DIR in listing[0]['children']
    assert PACKAGE_JSON in listing[0]['children']
    assert WHAT_DIR in listing[0]['children']
    assert MOUSE_DIR in listing[0]['children']


def test_unarchive_pst(taskmanager):
    pst_blob = models.Blob.create_from_file(JANE_DOE_PST)
    listing_blob = archives.unarchive(pst_blob)
    with listing_blob.open() as f:
        listing = json.load(f)

    EML_NUMBER_5 = {
        "type": "file",
        "name": "5.eml",
        "blob_pk": "9c007ccf1720d6279fc64389321fd83e053c9c4abc885a1745e9bc6793d515c9"
    }

    [root_dir] = listing
    assert root_dir['name'] == 'pst-test-2@aranetic.com'
    assert root_dir['type'] == 'directory'
    assert len(root_dir['children']) == 3


def test_unarchive_tar_gz(taskmanager):
    tar_gz_blob = models.Blob.create_from_file(TAR_GZ)
    listing_blob = archives.unarchive(tar_gz_blob)
    with listing_blob.open() as f:
        listing = json.load(f)

    [tar_file] = listing
    assert tar_file['type'] == 'file'

    tar_blob = models.Blob.objects.get(pk=tar_file['blob_pk'])
    listing_blob = archives.unarchive(tar_blob)
    with listing_blob.open() as f:
        listing = json.load(f)

    assert set(f['name'] for f in listing) == {
        'sample (1).doc',
        'Sample_BulletsAndNumberings.docx',
        'cap33.pdf',
    }


def test_unarchive_rar(taskmanager):
    rar = models.Blob.create_from_file(RAR)
    listing_blob = archives.unarchive(rar)
    with listing_blob.open() as f:
        listing = json.load(f)

    assert set(f['name'] for f in listing) == {
        'sample (1).doc',
        'Sample_BulletsAndNumberings.docx',
        'cap33.pdf',
    }


def test_create_archive_files(taskmanager):
    zip_blob = models.Blob.create_from_file(ZIP_DOCX)
    listing_blob = archives.unarchive(zip_blob)

    col = models.Collection.objects.create(
        name='testdata',
        root=Path(settings.SNOOP_TESTDATA) / 'data',
    )
    zip_parent_dir = models.Directory.objects.create(
        collection=col,
    )
    zip_file = models.File.objects.create(
        collection=col,
        name=JERRY_ZIP.name,
        parent_directory=zip_parent_dir,
        ctime=time_from_unix(0),
        mtime=time_from_unix(0),
        size=0,
        original=zip_blob,
    )

    filesystem.create_archive_files(zip_file.pk, listing_blob)

    assert models.Directory.objects.count() == 2  # root and fake dir
    assert models.File.objects.count() == 3  # zip, docx and doc

    file_names = set(f.name for f in models.File.objects.all())
    assert file_names == {'jerry.zip', 'AppBody-Sample-English.docx', 'sample.doc'}

    assert models.Directory.objects.get(container_file__isnull=False).container_file == zip_file


def test_unarchive_mbox(taskmanager):
    mbox_blob = models.Blob.create_from_file(SHAPELIB_MBOX)
    listing_blob = archives.unarchive(mbox_blob)
    with listing_blob.open() as f:
        listing = json.load(f)

    assert len(listing) == 240
    assert listing[0] == {
        'children': [
            {'blob_pk': 'ef36873c10e6037f5ae889ecef319a0f4458fbf3ad35a34bf73d74b203a0ebc0',
             'name': '0026476a20bfbd08714155bb66f0b4feb2d25c1c.eml',
             'type': 'file'},
            {'blob_pk': '6ed1e68b8167272461a1fd019a0320e662d71e2276d59b963ea0fee94d09e8db',
             'name': '008451a05e1e7aa32c75119df950d405265e0904.eml',
             'type': 'file'},
            {'blob_pk': 'a4b9a0d122cdd3c4f8a2e7749485d6f6d0cbf443557b6c02b227b9c5e7e2352b',
             'name': '00a8a5c3f7bac086c6df1a59b7da7e26eee029a1.eml',
             'type': 'file'},
            {'blob_pk': 'abeebc643dfd95ca4d97de177dc20ca508d030ee970281e073ecd74b983a3df4',
             'name': '00f7eea0d077127d2045e251487cfe61189614c7.eml',
             'type': 'file'}],
        'name': '00',
        'type': 'directory'}
