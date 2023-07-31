"""Module for managing S3-fuse mounts.

A limited number of mounts can be used at one time.

To decide what mounts are kept, we use a Least Recently Used
caching strategy.
"""

import time
import signal
import logging
import psutil
import shutil
import os
import json
import fcntl
from contextlib import contextmanager
import subprocess

from django.conf import settings


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def clean_makedirs(path):
    """Helper function that works like `os.makedirs(path, exist_ok=True)`,
    but also takes care to remove any file that might be at the path instead of a folder.
    """
    if os.path.isdir(path):
        return
    try:
        os.makedirs(path, exist_ok=True)
        return
    except OSError:
        assert os.path.exists(path), "dir not created after os.makedirs()!"

        if not os.path.isdir(path):
            logger.warning('found file/socket instead of directory at %s... removing', path)
            os.remove(path)
            os.makedirs(path, exist_ok=True)
            assert os.path.isdir(path), "dir not created after removing file and running os.makedirs()!"


def timestamp():
    """Returns current timestamp float for the mount LRU Cache."""

    return time.time()


def get_mount(mount_name, bucket, mount_mode, access_key, secret_key, address):
    """Ensure requested S3fs is mounted, while also
    unmounting least recently used mounts over the limit."""

    clean_makedirs(settings.SNOOP_S3FS_MOUNT_DIR)

    mount_info_path = os.path.join(settings.SNOOP_S3FS_MOUNT_DIR, 'mount-info.json')
    base_path = os.path.join(settings.SNOOP_S3FS_MOUNT_DIR, mount_name)
    target_path = os.path.join(base_path, 'target')
    cache_path = os.path.join(base_path, 'cache')
    logfile_path = os.path.join(base_path, 'mount-log.txt')
    password_file_path = os.path.join(base_path, 'password-file')

    clean_makedirs(base_path)

    # write password file
    with open(password_file_path, 'wb') as pass_temp:
        password_file = pass_temp.name
        subprocess.check_call(f"chmod 600 {password_file}", shell=True)
        pass_str = (access_key + ':' + secret_key).encode('latin-1')
        pass_temp.write(pass_str)
        pass_temp.close()

    with open_exclusive(mount_info_path, 'a+') as f:
        f.seek(0)
        old_info_str = f.read()

        logger.info('read mount info: %s', old_info_str)
        if old_info_str:
            try:
                old_info = json.loads(old_info_str)
            except Exception as e:
                logger.warning('old mount info corrupted: %s', e)
                old_info = dict()
        else:
            old_info = dict()

        new_info = adjust_s3_mounts(
            mount_name, old_info,
            bucket, mount_mode, cache_path, password_file_path, address, target_path, logfile_path
        )

        f.seek(0)
        f.truncate()
        json.dump(new_info, f)

    return target_path


@contextmanager
def open_exclusive(file_path, *args, **kwargs):
    """Context manager that uses exclusive blocking flock
    to ensure singular access to opened file."""

    def lock_file(fd):
        fcntl.flock(fd, fcntl.LOCK_EX)

    def unlock_file(fd):
        fcntl.flock(fd, fcntl.LOCK_UN)

    f = open(file_path, *args, **kwargs)
    lock_file(f.fileno())
    try:
        yield f
    finally:
        f.flush()
        os.fsync(f.fileno())
        unlock_file(f.fileno())
        f.close()


def adjust_s3_mounts(mount_name, old_info,
                     bucket, mount_mode, cache_path,
                     password_file_path, address, target_path, logfile_path):
    """Implement Least Recently used cache for the S3 mounts.

    - check all mount PIDs: if any process is dead, remove from list
    - if mount exists in list, update timestamp, return
    - if mount doesn't exist, then:
      - create new mount, save PID
      - create new entry with pid and timestamp
      - if above mount limit, then send signals to unmount and stop
    """

    # check all mount PIDs: if any process is dead, remove from list
    pids_alive = {p.pid for p in psutil.process_iter()}
    new_info = dict(old_info)
    for key, value in list(old_info.items()):
        if value['pid'] not in pids_alive:
            logger.info('old mount dead, key=%s', key)
            del new_info[key]

    # if mount exists in list, update timestamp, return
    if mount_name in new_info:
        logger.info('found old mount still alive: %s', mount_name)
        new_info[mount_name]['timestamp'] = timestamp()
        return new_info

    # create new mount
    logger.info('creating new mount: %s', mount_name)
    clean_makedirs(target_path)
    clean_makedirs(cache_path)
    pid = mount_s3fs(bucket, mount_mode, cache_path, password_file_path, address, target_path, logfile_path)

    # create new entry with pid and timestamp
    new_info[mount_name] = {
        'pid': pid, 'timestamp': timestamp(),
        'target': target_path, 'cache': cache_path,
    }

    # if above mount limit, then send signals to unmount and stop
    if len(new_info) > settings.SNOOP_S3FS_MOUNT_LIMIT:
        count_mounts_to_remove = len(new_info) - settings.SNOOP_S3FS_MOUNT_LIMIT
        mounts_sorted_by_timestamp = sorted(list(new_info.keys()), key=lambda x: new_info[x]['timestamp'])
        mounts_to_stop = mounts_sorted_by_timestamp[:count_mounts_to_remove]
        for mount in mounts_to_stop:
            pid = new_info[mount]['pid']
            target = new_info[mount]['target']
            cache = new_info[mount].get('cache')
            logger.info('removing old mount: pid=%s target=%s', pid, target)

            try:
                umount_s3fs(target)
            except Exception as e:
                logger.exception('failed to run "umount" for target=%s (%s)', target, e)

            try:
                os.kill(pid, signal.SIGSTOP)
            except Exception as e:
                logger.exception('failed to send SIGSTOP to mount, pid=%s (%s)', pid, e)

            try:
                os.kill(pid, signal.SIGKILL)
            except Exception as e:
                logger.exception('failed to send SIGKILL to mount, pid=%s (%s)', pid, e)

            if target:
                try:
                    os.rmdir(target)
                except Exception as e:
                    logger.exception('Failed to os.rmdir() the target directory %s (%s)', target, e)

            if cache:
                try:
                    shutil.rmtree(cache)
                except Exception as e:
                    logger.exception('Failed to shutil.rmtree() the cache directory %s (%s)', cache, e)

    return new_info


def mount_s3fs(bucket, mount_mode, cache, password_file, address, target, logfile):
    """Run subprocess to mount s3fs disk to target. Will wait until completed."""

    # unused options:
    #    -o use_cache={cache} \\
    #    -o multipart_copy_size=32 \\
    # don't use cache -- it downloads whole file when requested, which
    # does not work on very large archives (would need same amount of temp space)

    cmd_bash = f"""
    nohup s3fs \\
        -f \\
        -o {mount_mode} \\
        -o allow_other \\
        -o max_dirty_data=64 \\
        -o passwd_file={password_file}  \\
        -o use_path_request_style  \\
        -o url=http://{address} \\
        {bucket} {target} > {logfile} \\
        2>&1 & echo $!
    """
    logger.info('running s3fs process: %s', cmd_bash)
    output = subprocess.check_output(cmd_bash, shell=True)
    pid = int(output)
    logger.info('s3fs process started with pid %s', pid)
    return pid


def umount_s3fs(target):
    """Run subprocess to umount s3fs disk from target. Will wait until completed."""

    subprocess.run(f"umount {target}", shell=True, check=False)
    if os.listdir(target):
        subprocess.check_call(f"""
            umount {target} || umount -l {target} || true;
        """, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,)
        return
