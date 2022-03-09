"""Utilities to manage files, like removing/renaming etc."""

import shutil
import logging
from .. import models
from .. import collections
import os
from pathlib import Path


log = logging.getLogger(__name__)


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


def child_directories(dir, leafs):
    if not dir.child_directory_set.all():
        leafs.add(dir)
    else:
        for entry in dir.child_directory_set.all():
            leafs.union(child_directories(entry, leafs))

    return leafs


def delete_parents(dir, target_dir):
    if dir == target_dir:
        return
    else:
        log.info('Deleted: ' + dir.name)
        parent = dir.parent
        if dir.pk:  # we might have deleted the parent earlier as some other dirs parent
            dir.delete()
        delete_parents(parent, target_dir)


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


def delete_dir(dir_pk):
    directory = models.Directory.objects.get(pk=int(dir_pk))
    original_disk_path = disk_path(directory.name)

    for leaf in child_directories(directory, set()):
        delete_parents(leaf, directory)

    shutil.rmtree(original_disk_path)
    directory.delete()
    log.info('Successfully deleted "' + directory.name + '" and all subdirectories!')


def move_dir(dir_pk, new_path):
    new_disk_path = disk_path(new_path)
    print('New disk path: ' + str(new_disk_path))
    directory = models.Directory.objects.get(pk=int(dir_pk))
    print(directory)
    original_disk_path = disk_path(str(directory))
    print('Original disk path: ' + str(original_disk_path))
    directory.parent_directory = create_directory_objects(Path(new_path))
    directory.save()
    shutil.move(original_disk_path, new_disk_path)
