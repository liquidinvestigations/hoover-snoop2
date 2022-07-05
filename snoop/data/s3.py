"""Module for managing S3-fuse mounts.

A limited number of mounts can be used at one time.

To decide what mounts are kept, we use a Least Recently Used
caching strategy.
"""

import time
import signal
import logging
import psutil
import os
import json
import fcntl
from contextlib import contextmanager
import subprocess

from django.conf import settings


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def timestamp():
    """Returns current timestamp float for the mount LRU Cache."""

    return time.time()


def get_mount(mount_name, bucket, mount_mode, access_key, secret_key, address):
    """Ensure requested S3fs is mounted, while also
    unmounting least recently used mounts over the limit."""

    os.makedirs(settings.SNOOP_S3FS_MOUNT_DIR, exist_ok=True)
    mount_info_path = os.path.join(settings.SNOOP_S3FS_MOUNT_DIR, 'mount-info.json')
    base_path = os.path.join(settings.SNOOP_S3FS_MOUNT_DIR, mount_name)
    target_path = os.path.join(base_path, 'target')
    cache_path = os.path.join(base_path, 'cache')
    logfile_path = os.path.join(base_path, 'mount-log.txt')
    password_file_path = os.path.join(base_path, 'password-file')

    os.makedirs(base_path, exist_ok=True)
    os.makedirs(target_path, exist_ok=True)
    os.makedirs(cache_path, exist_ok=True)

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
            old_info = json.loads(old_info_str)
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
    pid = mount_s3fs(bucket, mount_mode, cache_path, password_file_path, address, target_path, logfile_path)

    # create new entry with pid and timestamp
    new_info[mount_name] = {'pid': pid, 'timestamp': timestamp(), 'target': target_path}

    # if above mount limit, then send signals to unmount and stop
    if len(new_info) > settings.SNOOP_S3FS_MOUNT_LIMIT:
        count_mounts_to_remove = len(new_info) - settings.SNOOP_S3FS_MOUNT_LIMIT
        mounts_sorted_by_timestamp = sorted(list(new_info.keys()), key=lambda x: new_info[x]['timestamp'])
        mounts_to_stop = mounts_sorted_by_timestamp[:count_mounts_to_remove]
        for mount in mounts_to_stop:
            pid = new_info[mount]['pid']
            target = new_info[mount]['target']
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
        -o dbglevel=info \\
        -o curldbg \\
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


def lock_mount_7z_fuse(archive_path, mount_target, logfile):
    """Mount new 7z archive while using a lock-protected JSON database
    to remember created PIDs in the system.

    This function also implements a clean-up of outdated / dead / defunct proceses.
    """
    os.makedirs(settings.SNOOP_S3FS_MOUNT_DIR, exist_ok=True)
    mount_info_path = os.path.join(settings.SNOOP_S3FS_MOUNT_DIR, '7z-mount-info.json')

    with open_exclusive(mount_info_path, 'a+') as f:
        f.seek(0)
        old_info_str = f.read()

        logger.info('read mount info: %s', old_info_str)
        if old_info_str:
            old_info = json.loads(old_info_str)
        else:
            old_info = dict()

        new_info, new_pid = adjust_7z_mounts(
            old_info,
            archive_path, mount_target, logfile
        )

        f.seek(0)
        f.truncate()
        json.dump(new_info, f)

    # wait for mount to appear
    TIME_LIMIT = 60
    t0 = time.time()
    while not os.listdir(mount_target):
        # check PID if still alive
        if not psutil.pid_exists(new_pid):
            raise RuntimeError('7z-fuse process crashed!')
        if time.time() - t0 > TIME_LIMIT:
            umount_7z_fuse(new_pid, mount_target)
            raise RuntimeError('7z-fuse process failed to start in time!')
        time.sleep(.01)
    logger.info('mount done after %s', round(time.time() - t0, 3))
    return new_pid


def adjust_7z_mounts(old_info, archive_path, mount_target, logfile):
    """Adjust mount directory by adding a new mount and removing old ones.

    Holds info in a JSON file keyed by mount process PID as string.


    Steps:
        - for each old entry:
            - if pid is dead, remove entry
            - if ppid is dead, umount pid/target
        - add new entry
    """
    # clear out old entries
    pids_alive = {p.pid for p in psutil.process_iter()}
    new_info = {str(k): v for k, v in old_info.items()}
    for old_entry in list(new_info.values()):
        if old_entry['pid'] not in pids_alive:
            logger.info('mount process deleted from json: pid=%s', old_entry['pid'])
            del new_info[str(old_entry['pid'])]
            continue
        if old_entry['ppid'] not in pids_alive:
            umount_7z_fuse(old_entry['pid'], old_entry['target'])

    # make new entry
    ppid = os.getpid()
    pid = make_mount_7z_fuse(archive_path, mount_target, logfile)
    new_entry = {'pid': pid, 'ppid': ppid, 'target': mount_target, 'time': time.time()}
    new_info[str(pid)] = new_entry
    return new_info, pid


def make_mount_7z_fuse(archive_path, mount_target, logfile):
    """Make new 7z-fuse mount process and return its pid."""

    cmd_bash = f"""
        nohup fuse_7z_ng -f -o ro {archive_path} {mount_target} > {logfile} 2>&1 & echo $!
    """

    logger.info('running fuse 7z process: %s', cmd_bash)
    output = subprocess.check_output(cmd_bash, shell=True)
    pid = int(output)
    logger.info('7z fuse process started with pid %s', pid)

    return pid


def umount_7z_fuse(pid, target):
    """Forcefully stop 7z user mount by using umount, fusermount and kill operations."""

    logger.info('unmounting fuse 7z process: pid=%s target=%s ....', pid, target)
    try:
        subprocess.run(
            ['umount', target],
            check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    except Exception as e:
        logger.warning('failed to run umount path %s, target pid=%s (%s)', target, pid, str(e))

    try:
        subprocess.run(
            ['fusermount', '-u', target],
            check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    except Exception as e:
        logger.warning('failed to run fusermount -u path %s, target pid=%s (%s)', target, pid, str(e))

    try:
        os.kill(pid, signal.SIGSTOP)
    except Exception as e:
        logger.warning('failed to send SIGSTOP to mount, pid=%s (%s)', pid, str(e))

    try:
        os.kill(pid, signal.SIGKILL)
    except Exception as e:
        logger.warning('failed to send SIGKILL to mount, pid=%s (%s)', pid, str(e))

    try:
        os.rmdir(target)
    except Exception as e:
        logger.warning('failed to delete mount directory, pid=%s (%s)', pid, str(e))

    logger.info('unmount finished: fuse 7z pid=%s target=%s ....', pid, target)
