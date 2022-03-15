import pytest
import os
import shutil
from snoop.data.file_management import file_management
from snoop.data import models
from pathlib import Path
from conftest import TESTDATA

pytestmark = [pytest.mark.django_db]

TEST_FILE = TESTDATA / './disk-files/images/bikes.jpg'


def mock_function(*args, **kwargs):
    return


def test_delete_file(fakedata, monkeypatch):
    def mock_unlink(*args, **kwargs):
        return

    monkeypatch.setattr(Path, 'unlink', mock_function)

    root = fakedata.init()

    with TEST_FILE.open('rb') as f:
        blob = fakedata.blob(f.read())

    fakedata.file(root, 'bikes.jpg', blob)

    file_management.delete(blob.pk)

    assert not models.File.objects.filter(blob=blob)
    assert not models.Blob.objects.filter(pk=blob.pk)


def test_rename_file(fakedata, monkeypatch):

    monkeypatch.setattr(Path, 'mkdir', mock_function)
    monkeypatch.setattr(os.path, 'exists', mock_function)
    monkeypatch.setattr(shutil, 'move', mock_function)

    root = fakedata.init()

    with TEST_FILE.open('rb') as f:
        blob = fakedata.blob(f.read())

    testfile = fakedata.file(root, 'bikes.jpg', blob)

    file_management.rename(blob.pk, '/', 'bikes2.jpg')

    testfile.refresh_from_db()

    assert testfile.name == 'bikes2.jpg'

    file_management.rename(blob.pk, '/testdir/', 'bikes3.jpg')

    testfile.refresh_from_db()

    assert testfile.parent == models.Directory.objects.get(name_bytes='testdir'.encode('utf-8'), parent_directory=root)


def test_delete_dir(fakedata, monkeypatch):

    monkeypatch.setattr(shutil, 'rmtree', mock_function)

    root = fakedata.init()

    testdir = fakedata.directory(root, 'testdir')

    with TEST_FILE.open('rb') as f:
        blob = fakedata.blob(f.read())

    testfile = fakedata.file(testdir, 'bikes.jpg', blob)

    file_management.delete_dir(testdir.pk)

    assert not models.Directory.objects.filter(pk=testdir.pk)
    assert not models.File.objects.filter(pk=testfile.pk)
    assert not models.Blob.objects.filter(pk=blob.pk)


def test_rename_dir(fakedata, monkeypatch):

    monkeypatch.setattr(Path, 'mkdir', mock_function)
    monkeypatch.setattr(os.path, 'exists', mock_function)
    monkeypatch.setattr(shutil, 'move', mock_function)

    root = fakedata.init()

    testdir = fakedata.directory(root, 'testdir')

    file_management.move_dir(testdir.pk, '/testdir3')

    testdir.refresh_from_db()

    assert testdir.parent_directory == models.Directory.objects.get(name_bytes='testdir3'.encode('utf-8'),
                                                                    parent_directory=root)
