"""Utilities to manage files, like removing/renaming etc."""

import shutil
import logging
from .. import models
from .. import collections
import os
from pathlib import Path


log = logging.getLogger(__name__)


def disk_path(path, *parts):
    '''Utility function to build the disk path from a files or directories path.

    Args:
        path (str or pathlike): A path for which the full disk path is needed.
        *parts (str or pathlike): Optional additional parts, e.g. a filename.

    Returns:
       The full disk path of the given path and all parts, joined as a Path object.
    '''

    BASE_DIR = collections.current().data_path
    return BASE_DIR.joinpath(path.lstrip('/'), *parts)


def create_directory_objects(path):
    '''Utility function to create the directory structure for the given path in the database.

    Args:
        path (pathlike): A directory path (no filename).

    Returns:
        The last directory in the path (leaf).
    '''

    if path == path.parent:
        directory, _ = models.Directory.objects.get_or_create(name_bytes=path.name.encode('utf-8'))
        return directory
    else:
        directory, _ = models.Directory.objects.get_or_create(
            name_bytes=path.name.encode('utf-8'),
            parent_directory=create_directory_objects(path.parent)
        )
        return directory


def child_directories(dir, leafs=set()):
    '''Utility function to get all leaf sub-directories of a given directory.

    Args:
        dir (directory object): A directory.
        leafs (set): A set (empty by default) that gets filled throughout the recursion.

    Returns:
        The set of directories containing all the leaf directories.
    '''

    if not dir.child_directory_set.all():
        leafs.add(dir)
    else:
        for entry in dir.child_directory_set.all():
            leafs.union(child_directories(entry, leafs))

    return leafs


def delete_files(dir):
    '''Utility function to delete files and their blobs from a directory.

    Args:
        dir (directory object): The directory containing the files.
    '''

    files = models.File.objects.filter(parent_directory=dir)
    for f in files:
        orig_blob = f.original
        blob = f.blob
        f.delete()
        orig_blob.delete()
        blob.delete()


def delete_parents(dir, target_dir):
    '''Utility function to delete all parents of a given directory up to a target directory.

    Args:
        dir (directory object): A directory for which the parents will be deleted.
        target_dir (directory object): A target directory, where the deletion stops.
    '''

    if dir == target_dir:
        return
    else:
        log.info('Deleted: ' + dir.name)
        parent = dir.parent
        if dir.pk:  # we might have deleted the parent earlier as some other dirs parent
            delete_files(dir)
            dir.delete()
        delete_parents(parent, target_dir)


def rename(blob_hash, new_path, new_filename):
    '''Function to rename a file on disk and in the database.

    This will rename or move the file and create target directory structure, if it
    doesn't exist. The path/name will be changed both on disk and in the directory structure
    in the database.

    Args:
        blob_hash: Hash of a blob for which the corresponding file will be renamed.
        new_path (str): The new directory path of the file (without filename).
        new_filename (str): The new filename with extension.
    '''
    new_disk_path = disk_path(new_path, new_filename)
    file = models.File.objects.get(original_id=blob_hash)
    original_disk_path = disk_path(str(file.parent_directory), file.name)
    file.parent_directory = create_directory_objects(Path(new_path))
    file.name_bytes = new_filename.encode('utf-8')
    file.save()
    if not os.path.exists(new_disk_path.parent):
        new_disk_path.parent.mkdir(exist_ok=True, parents=True)
    shutil.move(original_disk_path, new_disk_path)


def delete(blob_hash):
    '''Function to delete a file on disk and in the database.

    Args:
        blob_hash: Hash of a blob for which the corresponding file will be removed.
    '''
    file = models.File.objects.get(original_id=blob_hash)
    original_disk_path = disk_path(str(file.parent_directory), file.name)  # make this a function eventually
    original_disk_path.unlink()
    file.delete()
    models.Blob.objects.get(pk=blob_hash).delete()


def delete_dir(dir_pk):
    '''Function to delete a directory on disk and in the database.

    This will remove the directory and all its sub-directories on disk and in
    the database. To delete them from the database, first all the leafs are
    retrieved and deletion starts from there up until the given directory is reached.

    Args:
        dir_pk (str or int): Primary key of the directory that will be removed.
    '''
    directory = models.Directory.objects.get(pk=int(dir_pk))
    original_disk_path = disk_path(str(directory))

    for leaf in child_directories(directory, set()):
        delete_parents(leaf, directory)

    delete_files(directory)
    directory.delete()
    shutil.rmtree(original_disk_path)
    log.info('Successfully deleted "' + directory.name + '" and all subdirectories!')

    # TODO delete all blobs in the directory and its subdirectories when deleting the file.
    # Also delete all File objects for the directories in the database.


def move_dir(dir_pk, new_path):
    '''Move a directory on disk and in the database.

    This will move a directory. In the database it will create the new path, if it doesn't exist
    and set the directories parent directory to the new value. On disk it will also create the new
    path if it doesn't exist and then move the directory.

    Args:
        dir_pk (str or int): Primary key of the directory that will be moved.
        new_path (str): Path of a directory where the given directory should be moved to.
    '''
    new_disk_path = disk_path(new_path)
    print('New disk path: ' + str(new_disk_path))
    directory = models.Directory.objects.get(pk=int(dir_pk))
    print(directory)
    original_disk_path = disk_path(str(directory))
    print('Original disk path: ' + str(original_disk_path))
    directory.parent_directory = create_directory_objects(Path(new_path))
    directory.save()
    if not os.path.exists(new_disk_path):
        new_disk_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(original_disk_path, new_disk_path)
