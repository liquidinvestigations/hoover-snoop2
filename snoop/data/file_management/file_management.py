"""Utilities to manage files, like removing/renaming etc."""

import shutil
from .. import models
from .. import collections
import os
from pathlib import Path


def disk_path(path, *parts):
    BASE_DIR = collections.current().data_path
    return BASE_DIR.joinpath(path.lstrip('/'), *parts)


def create_directory_objects(path):
    if path == path.parent:
        directory, _ = models.Directory.objects.get_or_create(name_bytes=path.name.encode('utf-8'))
        return directory
    else:
        directory, _ = models.Directory.objects.get_or_create(
            name_bytes=path.name.encode('utf-8'),
            parent_directory=create_directory_objects(path.parent)
        )
        return directory


def rename(blob_hash, new_path, new_filename):
    new_disk_path = disk_path(new_path, new_filename)
    file = models.File.objects.get(original_id=blob_hash)
    original_disk_path = disk_path(str(file.parent_directory), file.name)
    file.parent_directory = create_directory_objects(Path(new_path))
    file.name_bytes = new_filename.encode('utf-8')
    file.save()
    if not os.path.exists(new_disk_path.parent):
        new_disk_path.parent.mkdir(parents=True)
    shutil.move(original_disk_path, new_disk_path)


def delete(blob_hash):
    file = models.File.objects.get(original_id=blob_hash)
    original_disk_path = disk_path(str(file.parent_directory), file.name)  # make this a function eventually
    original_disk_path.unlink()
    file.delete()
