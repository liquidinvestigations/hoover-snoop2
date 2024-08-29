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
import subprocess
import sys

from django.conf import settings

from . import tracing
from .utils import open_exclusive


logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
logger.setLevel(logging.INFO)
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - [%(levelname)s] - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)
tracer = tracing.Tracer(__name__)


def clean_makedirs(path):
    """Helper function that works like `os.makedirs(path, exist_ok=True)`,
    but also takes care to remove any file that might be at the path instead of a folder.
    """
    RETRIES = 3
    SLEEP = 0.05
    for retry in range(RETRIES):
        try:
            if os.path.isdir(path):
                return

            # try first makedirs, to get the parents
            try:
                os.makedirs(path, exist_ok=True)
                return
            except OSError:
                pass

            # if it's a normal file, remove that
            if os.path.exists(path) and not os.path.isdir(path):
                try:
                    os.remove(path)
                except Exception as e:
                    logger.exception(e)

            if os.path.exists(path):
                logger.error('os.remove() did not remove the file!')

            try:
                os.makedirs(path, exist_ok=True)
                return
            except OSError:
                assert os.path.exists(path), "dir not created after second os.makedirs()!"

                if not os.path.isdir(path):
                    os.makedirs(path, exist_ok=True)
                    assert os.path.isdir(path), \
                        "dir not created after removing file and running os.makedirs()!"
        except Exception as e:
            logger.warning('retrying clean_makedirs() %s/%s %s', retry, RETRIES, str(e))
            time.sleep(SLEEP)


WORKER_PROCESS_INDEX = -1


def _get_worker_process_index():
    """Returns a numerical index that is stable through worker restarts.

    We want the same IDs to be reused by processes. PIDs are unadequate for this
    role because they are managed to avoid collisions.

    I couldn't get Celery or its library Billard to get me a stable worker ID.
    """

    # for Gunicorn workers, we get this from an env
    if (os.getenv("GUNICORN_WORKER_ID") or '').strip():
        return int(os.getenv("GUNICORN_WORKER_ID"))

    clean_makedirs(settings.SNOOP_S3FS_MOUNT_DIR)
    worker_pid_list_file = os.path.join(
        settings.SNOOP_S3FS_MOUNT_DIR,
        'worker-process-index-list.json'
    )
    worker_pid_list = []
    with open_exclusive(worker_pid_list_file, 'a+') as f:
        f.seek(0)
        info_str = f.read()
        if info_str:
            try:
                worker_pid_list = json.loads(info_str)
            except Exception as e:
                logger.debug('cannot parse pids: %s %s %s',
                             worker_pid_list_file, info_str, str(e))
        # check if our pid is in the list; if it is, return the index
        current_pid = os.getpid()
        if current_pid in worker_pid_list:
            return worker_pid_list.index(current_pid)

        # check if any other process is dead; if it is, replace its place in the list
        all_pids = {p.pid for p in psutil.process_iter()}
        for i, old_pid in enumerate(worker_pid_list):
            if old_pid not in all_pids:
                worker_pid_list[i] = current_pid
                break
        else:
            # we did not find any dead process in the list
            worker_pid_list.append(current_pid)

        # overwrite data
        f.seek(0)
        f.truncate()
        json.dump(worker_pid_list, f)
        return worker_pid_list.index(current_pid)


def refresh_worker_index():
    global WORKER_PROCESS_INDEX
    WORKER_PROCESS_INDEX = _get_worker_process_index()
    logger.debug(
        'WORKER PROCESS INDEX = %s  (pid=%s  args=%s)',
        WORKER_PROCESS_INDEX,
        os.getpid(),
        sys.argv,
    )


def timestamp():
    """Returns current timestamp float for the mount LRU Cache."""

    return time.time()


def _get_worker_base_path():
    """Returns a unique path for each subprocess where we place mounts."""

    worker_base_path = os.path.join(
        settings.SNOOP_S3FS_MOUNT_DIR,
        str(WORKER_PROCESS_INDEX),
    )
    clean_makedirs(worker_base_path)
    return worker_base_path


def clear_mounts():
    """Unmount all S3 volumes and clear out the metadata and logs.
    Used when Celery process restarts."""

    worker_base_path = _get_worker_base_path()
    if not os.path.isdir(worker_base_path):
        return

    mount_info_path = os.path.join(worker_base_path, 'mount-info.json')
    mount_info = {}
    if os.path.isfile(mount_info_path):
        with open_exclusive(mount_info_path, 'a+') as f:
            f.seek(0)
            info_str = f.read()

            logger.debug('read mount info: %s', info_str)
            if info_str:
                try:
                    mount_info = json.loads(info_str)
                except Exception as e:
                    logger.debug('clear mounts info corrupted: %s', e)
        for mount_value in mount_info.values():
            umount(mount_value['target'], mount_value['pid'])
        os.remove(mount_info_path)
    # in case json is kaputt, let's use `find` to try to unmount all targets
    targets = subprocess.check_output(
        (
            f'find "{worker_base_path}" '
            ' -mindepth 1 -maxdepth 3 -xdev -type d -name target'
        ),
        shell=True,
    ).decode('ascii').strip().splitlines()
    for target in targets:
        target = target.strip()
        if target:
            try:
                umount(target)
            except Exception as e:
                logger.warning('could not umount s3fs! %s', str(e))
                raise
    if not os.path.isdir(worker_base_path):
        return
    # keep using `find -xdev` to avoid deleting stuff inside mounts.
    subprocess.call(
        (
            f'find "{worker_base_path}" '
            ' -xdev -type f -delete'
        ),
        shell=True,
    )
    subprocess.call(
        (
            f'find "{worker_base_path}" '
            ' -xdev -type d -empty -delete'
        ),
        shell=True,
    )


def get_s3_mount(mount_name, bucket, mount_mode, access_key, secret_key, address):

    """Ensure requested S3fs is mounted, while also
    unmounting least recently used mounts over the limit."""

    paths = get_paths(mount_name)

    clean_makedirs(paths.get('base_path'))

    pass_str = (access_key + ':' + secret_key).encode('latin-1')
    write_password_file(paths.get('password_file_path'), pass_str)

    mount_args = {
        'bucket': bucket,
        'mount_mode': mount_mode,
        'password_file_path': paths.get('password_file_path'),
        'address': address,
        'target_path': paths.get('target_path'),
        'logfile_path': paths.get('logfile_path'),
        'check_pid': True,
    }

    adjust_and_write_mount_info(paths.get('mount_info_path'),
                                paths.get('target_path'),
                                mount_name,
                                mount_s3fs,
                                mount_args
                                )

    return paths.get('target_path')


def get_webdav_mount(mount_name, webdav_username, webdav_password, webdav_url):
    """Ensure requested webdav is mounted, while also
    unmounting least recently used mounts over the limit.

    Args:
        mount_name: Name of the mount (collection name).
        webdav_username: The username which is used for mounting the webdav files.
        webdav_password: The password which is used for mounting the webdav files.
        webdav_url: The url of the webdav share. It's the relative path after the base url.
          It looks like this: /remote.php/dav/files/user/collectionname

    Returns:
        The path in the filesystem where the webdav share is mounted.
    """
    paths = get_paths(mount_name)
    target_path = os.path.join(paths.get('target_path'), 'data')

    clean_makedirs(paths.get('base_path'))

    pass_str = (
        f'{target_path}'
        f' {webdav_username} {webdav_password}'
    ).encode('latin-1')

    write_password_file(paths.get('password_file_path'), pass_str)

    config_content = (
        f'[{target_path}]\n'
        f'secrets {paths.get("password_file_path")}'
    )
    config_file_path = os.path.join(paths.get('base_path'), 'config')
    write_config_file(config_file_path, config_content)

    mount_args = {
        'password_file_path': paths.get('password_file_path'),
        'target_path': target_path,
        'logfile_path': paths.get('logfile_path'),
        'configfile_path': config_file_path,
        'webdav_username': webdav_username,
        'webdav_url': webdav_url,
        'check_pid': False,
    }

    adjust_and_write_mount_info(paths.get('mount_info_path'),
                                target_path,
                                mount_name,
                                mount_webdav,
                                mount_args
                                )

    return paths.get('target_path')


def write_password_file(path, content):
    """Write the password file to disk and set permissions.

    Args:
        path: Path where the password file should be written to.
        content: Encoded content of the file as bytes.
    """
    with open(path, 'wb') as pass_temp:
        password_file = pass_temp.name
        subprocess.check_call(['chmod', '600', password_file])
        pass_str = content
        pass_temp.write(pass_str)
        pass_temp.close()


def get_paths(mount_name):
    """Get all the paths that are needed for mounting.

    Args:
        mount_name: Name of the mount (collection name).

    Returns:
        A dictionary with paths that are needed for mounting.
    """
    worker_base_path = _get_worker_base_path()
    mount_info_path = os.path.join(worker_base_path, 'mount-info.json')
    base_path = os.path.join(worker_base_path, mount_name)
    target_path = os.path.join(base_path, 'target')
    logfile_path = os.path.join('/tmp', f'{mount_name}' 'mount-log.txt')
    password_file_path = os.path.join(base_path, 'password-file')

    return {
        'worker_base_path': worker_base_path,
        'mount_info_path': mount_info_path,
        'base_path': base_path,
        'target_path': target_path,
        'logfile_path': logfile_path,
        'password_file_path': password_file_path,
    }


def adjust_and_write_mount_info(mount_info_path, target_path, mount_name, mount_func, mount_args):
    """Check the mount info file and adjust the mount if needed.

    This will open the info file and call the function that adjusts the mounts as needed.
    Then it will write the new mount info to the info file.

    Args:
        mount_info_path: Path to the file containing the mount info.
        target_path: Path where the filesystem should be mounted.
        mount_name: Name of the mount (collection name).
        mount_func: Function to use for mounting. Can be either
          mount_s3fs or mount_webdav.
        mount_args: Arguments that should be passed to the mounting function
          as a dictionary.

    """
    with open_exclusive(mount_info_path, 'a+') as f:
        f.seek(0)
        old_info_str = f.read()

        logger.debug('read mount info: %s', old_info_str)
        if old_info_str:
            try:
                old_info = json.loads(old_info_str)
            except Exception as e:
                logger.warning('old mount info corrupted: %s', e)
                old_info = dict()
        else:
            old_info = dict()

        t0 = time.time()

        new_info = adjust_mounts(
            mount_name, old_info,
            mount_func, mount_args
        )

        f.seek(0)
        f.truncate()
        json.dump(new_info, f)

        # wait until the mount was done correctly before returning - in total about 60s
        for retry in range(60):
            if target_is_mounted(target_path):
                dt = round(time.time() - t0, 3)
                logging.debug('mount %s working after %s sec', target_path, dt)
                break
            time.sleep(0.01 + 0.04 * retry)
        else:
            dt = round(time.time() - t0, 3)
            raise RuntimeError(f's3 mount did not start for {target_path} after {dt} sec!')


def write_config_file(config_file_path, config):
    """Write the content to the config file.

    Args:
        config_file_path: Path to the config file.
        config: Config content as a string.
    """
    with open(config_file_path, 'w') as confs_file:
        confs_file.write(config)


def target_is_mounted(path):
    """Returns True if the path is a linux mount point"""
    return 0 == subprocess.call(
        'findmnt ' + str(path),
        shell=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def adjust_mounts(mount_name, old_info,
                  mount_func, mount_args):
    """Implement Least Recently used cache for the S3 mounts.

    - check all mount PIDs for s3 mounts: if any process is dead, remove from list,
      webdav mounts run in background so we don't check the PID (check_pid argument)
    - if mount exists in list, update timestamp, return
    - if mount doesn't exist, then:
      - create new mount, save PID
      - create new entry with pid and timestamp
      - if above mount limit, then send signals to unmount and stop
    """

    def _clear_dead(info):
        # check all mount PIDs: if any process is dead, remove from list
        pids_alive = {p.pid for p in psutil.process_iter()}
        info = dict(info)
        for key, value in list(info.items()):
            if value['pid'] not in pids_alive:
                logger.info('old mount dead, key=%s', key)
                del info[key]
        return info

    # need to clear dead before checking, in case something died by itself
    if mount_args.get('check_pid'):
        new_info = _clear_dead(old_info)
    else:
        new_info = old_info

    # if mount exists in list, update timestamp, return
    if mount_name in new_info:
        logger.debug('found old mount still alive: %s', mount_name)
        new_info[mount_name]['timestamp'] = timestamp()
        return new_info

    # create new mount
    logger.info('creating new mount: %s', mount_name)
    clean_makedirs(mount_args.get('target_path'))

    pid = mount_func(mount_args)
    # create new entry with pid and timestamp
    new_info[mount_name] = {
        'pid': pid, 'timestamp': timestamp(),
        'target': mount_args.get('target_path'),
    }

    # if above mount limit, then send signals to unmount and stop
    if len(new_info) > settings.SNOOP_S3FS_MOUNT_LIMIT:
        count_mounts_to_remove = len(new_info) - settings.SNOOP_S3FS_MOUNT_LIMIT
        mounts_sorted_by_timestamp = sorted(list(new_info.keys()), key=lambda x: new_info[x]['timestamp'])
        mounts_to_stop = mounts_sorted_by_timestamp[:count_mounts_to_remove]
        for _ in range(2):
            for mount in mounts_to_stop:
                pid = new_info[mount]['pid']
                target = new_info[mount]['target']
                logger.info('removing old mount: pid=%s target=%s', pid, target)

                try:
                    umount(target, pid)
                except Exception as e:
                    logger.exception('failed to run "umount" for target=%s (%s)', target, e)
            time.sleep(0.001)

        new_info = _clear_dead(old_info)

    return new_info


def mount_s3fs(mount_args):
    """Run subprocess to mount s3fs disk to target. Will wait until completed."""

    # unused options:
    #    -o use_cache={cache} \\
    #    -o multipart_copy_size=32 \\
    # don't use cache -- it downloads whole file when requested, which
    # does not work on very large archives (would need same amount of temp space)

    cmd_bash = f"""
    s3fs \\
        -f \\
        -o {mount_args.get("mount_mode")} \\
        -o allow_other \\
        -o max_dirty_data=64 \\
        -o passwd_file={mount_args.get("password_file_path")}  \\
        -o use_path_request_style  \\
        -o url=http://{mount_args.get("address")} \\
        {mount_args.get("bucket")} {mount_args.get("target_path")} > {mount_args.get("logfile_path")} \\
        2>&1 & echo $!
    """
    logger.info('running s3fs process: %s', cmd_bash)
    tracer.count("mount_s3fs_start")
    output = subprocess.check_output(cmd_bash, shell=True)
    pid = int(output)
    logger.info('s3fs process started with pid %s', pid)
    return pid


def umount(target, pid=None):
    """Run subprocess to umount mount from target. Will wait until completed."""

    def _pid_alive():
        if pid:
            return pid in [p.pid for p in psutil.process_iter()]
        return False

    def _data_mounted():
        try:
            return bool(os.listdir(target))
        except Exception:
            return False

    subprocess.run(f"umount {target}", shell=True, check=False)
    subprocess.run(f"rmdir {target}", shell=True, check=False)
    if _pid_alive():
        subprocess.run(f"kill {pid}", shell=True, check=False)

    if not _pid_alive() and not _data_mounted() and not os.path.isdir(target):
        return

    for retry in range(10):
        subprocess.run(f"umount {target}", shell=True, check=False)
        subprocess.run(f"rmdir {target}", shell=True, check=False)

        if _pid_alive():
            try:
                os.kill(pid, signal.SIGSTOP)
            except Exception as e:
                logger.exception('failed to send SIGSTOP to mount, pid=%s (%s)', pid, e)

            try:
                os.kill(pid, signal.SIGKILL)
            except Exception as e:
                logger.exception('failed to send SIGKILL to mount, pid=%s (%s)', pid, e)

        if _data_mounted():
            subprocess.check_call(f"""
                umount {target} || umount -l {target} || true;
            """, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,)

        if not _data_mounted():
            try:
                os.rmdir(target)
            except Exception as e:
                logger.warning('Failed to os.rmdir() the target directory %s (%s)', target, e)

        if not _pid_alive() and not _data_mounted() and not os.path.isdir(target):
            tracer.count("umount_success")
            return

        time.sleep(0.05 + 0.05 * retry)

    tracer.count("umount_failed")
    raise RuntimeError(f'cannot remove old mounts! target={target}, pid={pid}')


def mount_webdav(mount_args):
    """Run subprocess to mount webdav share to target. Will wait until completed."""

    pid = -1
    if target_is_mounted(mount_args.get("target_path")):
        return pid

    cmd_bash = f"""
    mount -t davfs '{settings.SNOOP_NEXTCLOUD_URL}{mount_args.get("webdav_url")}' \\
        -o conf={mount_args.get("configfile_path")} \\
        {mount_args.get("target_path")} 2>&1 | tee {mount_args.get("logfile_path")} \\
        2>&1
    """
    logger.info('running davfs process: %s', cmd_bash)
    tracer.count("mount_webdav_start")
    subprocess.check_call(cmd_bash, shell=True)

    logger.info('davfs process started with pid %s', pid)
    return pid
