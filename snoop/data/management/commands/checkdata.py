""" Check data for orphaned Blobs or discrepancy between S3 and Database.

Optionally delete the Orphaned Database Blobs and S3 objects.
"""

import logging
import math
from datetime import timedelta

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db.models import Sum, Q
from django.utils import timezone
from django.db import transaction

import cachetools

from ... import collections
from ... import models
from ... import tasks
from ...utils import multiprocessing_generator
from ...logs import logging_for_management_command
from snoop.data.templatetags.pretty_size import pretty_size

log = logging.getLogger(__name__)


class Command(BaseCommand):
    "Check data for orphaned Blobs or discrepancy between S3 and Database."

    def add_arguments(self, parser):
        """Adds flag to switch between running collection workers and system workers."""
        parser.add_argument('--collection', default='__ALL__',
                            help="Check specific collection. By default, check all of them.")
        parser.add_argument('--min-age-hours', type=int, default=2,
                            help="Minimum object age (from date modified) for it to be checked/deleted.")
        parser.add_argument('--delete-orphaned', action='store_true', default=False,
                            help="Delete orphaned Blob objects from the database and S3.")
        parser.add_argument('--retry-tasks', action='store_true',
                            help='Retry tasks with missing result data from S3', default=False)

    def handle(self, *args, **options):
        """Runs workers for either collection processing or system tasks."""

        logging_for_management_command()
        collection = options['collection']
        if collection == '__ALL__':
            all_collections = list(collections.get_all())
        else:
            all_collections = [collections.get(collection)]

        for col in all_collections:
            with col.set_current():
                try:
                    log.info('\n============\nchecking collection %s\n==============', col.name)
                    errors = 0
                    errors += check_blobs_vs_s3(options['retry_tasks'], options['delete_orphaned'], options['min_age_hours'])
                    errors += check_blobs_orphaned(options['delete_orphaned'], options['min_age_hours'])
                    if errors > 0:
                        log.error('found %s errors in collection %s', errors, col.name)
                    else:
                        log.info('collection %s has no errors', col.name)
                except Exception as e:
                    log.exception(e)
                    log.error('failed to check data for collection %s', col.name)
                    continue


@cachetools.cached(cache=cachetools.LFUCache(maxsize=1000))
def _get_parent_dir_container_file_blob_id(_collection_name, directory_id):
    """If the directory has a parent directory that's got a container file,
    return that container file blob numerical id.

    Otherwise return NULL. Must cache root parts of directory tree to avoid extreme
    amounts of SQL queries.

    Args:
    - _collection_name: used to cache collections separately
    - directory_id: numerical pk for the directory - for simple cache keys
    """
    directory = models.Directory.objects.get(id=directory_id)
    if directory.container_file is not None:
        return [directory.container_file.original.pk, directory.container_file.blob.pk]
    if directory.parent_directory is not None:
        return _get_parent_dir_container_file_blob_id(_collection_name, directory.parent_directory.id)
    return None


def _get_related_for_missing_sha3_list(result_sha3_list):
    """Find (blobs, file_args, dir_args) that are related to these SHA3 hashes."""
    log.info('>>> retrying tasks for %s results', len(result_sha3_list))
    # get all db object IDs for faster queries
    missing_blobs = list(models.Blob.objects.filter(pk__in=result_sha3_list).values_list('pk', flat=True))
    log.info('> %s blobs', len(missing_blobs))

    # for files missing the originals, we need either a Walk task (if parent directory has parent directory)
    # or all the tasks on the container file (if parent directory has a container file).
    files_missing_original = list(
        models.File.objects.filter(Q(original__in=missing_blobs) | Q(blob__in=missing_blobs))
        .select_related('parent_directory').all()
    )
    log.info('> %s files missing original', len(files_missing_original))
    parent_dirs = list(f.parent_directory for f in files_missing_original)
    container_file_blobs = []
    for parent_dir in parent_dirs:
        container_files = _get_parent_dir_container_file_blob_id(collections.current().name, parent_dir.pk)
        if container_files:
            container_file_blobs.extend(container_files)
    container_file_blobs = list(set(container_file_blobs))

    missing_blobs += container_file_blobs
    log.info('> %s container file blobs found', len(container_file_blobs))

    # If digests.result/digests.extra_result is missing (need to reset the tasks for digest blob).
    digests_missing = models.Digest.objects.filter(Q(result__in=missing_blobs) | Q(extra_result__in=missing_blobs))
    missing_blobs += [d.blob.pk for d in digests_missing]
    log.info('> %s digests missing', len(digests_missing))

    # with expanded missing blobs list, get expanded File list
    files_missing_all = list(
        models.File.objects.filter(Q(original__in=missing_blobs) | Q(blob__in=missing_blobs))
        .select_related('parent_directory').all()
    )
    file_args = [[f.id] for f in files_missing_all]
    for f in files_missing_all:
        missing_blobs.append(f.blob.pk)
        missing_blobs.append(f.original.pk)
    dir_args = [[f.parent_directory.id] for f in files_missing_all]
    missing_blobs = list(set(missing_blobs))

    return (missing_blobs, file_args, dir_args)

def retry_tasks_for_results(result_sha3_list):
    """Find and retry tasks that result in these hashes."""
    FILE_TASK_FUNCS = [
        "emlx.reconstruct", "filesystem.create_archive_files",
        "filesystem.create_attachment_files", "filesystem.handle_file",
    ]
    DIR_TASK_FUNCS = ['filesystem.walk']

    # run sha3 collector twice to pick up any indirect connections
    (missing_blobs, file_args, dir_args) = _get_related_for_missing_sha3_list(result_sha3_list)
    (missing_blobs, file_args, dir_args) = _get_related_for_missing_sha3_list(missing_blobs)

    tasks_files = list(
        models.Task.objects.filter(args__in=file_args, func__in=FILE_TASK_FUNCS).values_list('pk', flat=True)
    )
    log.info('> %s file-related tasks missing', len(tasks_files))

    # with expanded file list, get expanded directory list
    tasks_walk = list(
        models.Task.objects.filter(args__in=dir_args, func__in=DIR_TASK_FUNCS).values_list('pk', flat=True)
    )
    log.info('> %s walk tasks to reset', len(tasks_walk))

    # ... Or the result or arg is missing (for any task type),
    tasks_missing_result = list(models.Task.objects.filter(result__in=missing_blobs).values_list('pk', flat=True))
    log.info('> %s tasks missing result', len(tasks_missing_result))
    tasks_missing_arg = list(models.Task.objects.filter(blob_arg__in=missing_blobs).values_list('pk', flat=True))
    log.info('> %s tasks missing arg', len(tasks_missing_arg))

    tasks_all = list(set(tasks_missing_result + tasks_missing_arg + tasks_walk + tasks_files))
    log.info('> %s initial tasks to check', len(tasks_all))

    # first tx: fetch non-locked non-pending tasks from our list
    with transaction.atomic(using=collections.current().db_alias):
        tasks_all = list((
            models.Task.objects
                .select_for_update(skip_locked=True)
                .filter(pk__in=tasks_all)
                .exclude(status=models.Task.STATUS_PENDING)
                .exclude(status=models.Task.STATUS_DEFERRED)
                .exclude(status=models.Task.STATUS_QUEUED)
        ).values_list('pk', flat=True))

    log.info('> %s tasks not locked or in progress', len(tasks_all))

    # fetch all parents of tasks (3 levels)
    for _ in range(3):
        parent_tasks = list(models.Task.objects.filter(next_set__next__in=tasks_all).values_list('pk', flat=True))
        if not parent_tasks:
            break
        log.info('> %s parent tasks for the tasks selected above', len(parent_tasks))
        tasks_all += parent_tasks
        tasks_all = list(set(tasks_all))
    log.info('> %s total tasks including parents. dropping locked tasks again...', len(tasks_all))

    # second tx: exclude locked again, and actually set it
    with transaction.atomic(using=collections.current().db_alias):
        qs = (
            models.Task.objects
                .select_for_update(skip_locked=True)
                .filter(pk__in=tasks_all)
                .exclude(status=models.Task.STATUS_PENDING)
                .exclude(status=models.Task.STATUS_DEFERRED)
                .exclude(status=models.Task.STATUS_QUEUED)
        )
        tasks.retry_tasks(qs, reset_fail_count=True, disable_queueing=True)


def s3_hash_size_iter(collection):
    """Generator that returns (sha, size, path) tuples in order from s3."""
    s3_object_iterator = settings.BLOBS_S3.\
        list_objects(collection.name, recursive=True)
    for obj in s3_object_iterator:
        if obj.is_dir:
            continue
        s3_sha3 = obj.object_name.replace('/', '')
        s3_size = obj.size
        yield s3_sha3, s3_size, obj.object_name

def db_hash_size_iter(collection):
    """Generator that returns (sha, size) tuples in order from db."""
    with collection.set_current():
        db_iterator = models.Blob.objects.filter(collection_source_key=b'')\
            .order_by('pk').values('pk', 'size', 'date_modified')
        for vals in db_iterator:
            yield vals['pk'], vals['size']


def check_blobs_vs_s3(retry_tasks, delete_from_s3=False, min_age_hours=2):
    """Check for differences between DB and S3 storage mediums.

    Args:
        - retry_tasks: bool: if set to True, find and retry all the tasks that
          can rebuild missing blobs.

    Report on:
        - S3 objects not in DB
        - DB objects not in S3
        - documents with differing sizes between DB and S3

    Returns:
        the number of distinct errors
    """
    task_results_to_retry = []
    size_mismatch_count = 0
    size_mismatch_total_size = 0
    missing_from_s3_count = 0
    missing_from_s3_total_size = 0
    missing_from_db_count = 0
    missing_from_db_total_size = 0

    PARALLEL_LOOKAHEAD = 10000
    GET_TIMEOUT = 900
    last_prefix = ''
    def _delete_from_s3_if_old_enough(s3_path):
        if not delete_from_s3:
            return
        try:
            stat = settings.BLOBS_S3.stat_object(
                collections.current().name,
                s3_path
            )
        except Exception as e:
            log.exception(e)
            log.warning('failed to stat s3 object before delete')
            return
        age_hours = (timezone.now() - stat.last_modified).total_seconds() / 3600
        if age_hours >= min_age_hours:
            log.debug('deleting s3 object with age %s hours: %s', age_hours, s3_path)
            settings.BLOBS_S3.remove_object(collections.current().name, s3_path)
        else:
            log.debug('s3 object too young to delete: %s', s3_path)

    with multiprocessing_generator.ParallelGenerator( \
                s3_hash_size_iter(collections.current()), \
                max_lookahead=PARALLEL_LOOKAHEAD, get_timeout=GET_TIMEOUT) \
            as s3_iter, \
            multiprocessing_generator.ParallelGenerator( \
                    db_hash_size_iter(collections.current()), \
                    max_lookahead=PARALLEL_LOOKAHEAD, get_timeout=GET_TIMEOUT) \
            as db_iter:

        s3_current = next(s3_iter, None)
        db_current = next(db_iter, None)

        s3_size = 0
        db_size = 0

        # while both iterators have items, compare the heads.
        # if the head item hashes are equal, check for size difference.
        # if they are different, then save the smaller one, and iterate the respective one.
        TASK_RETRY_BATCH_SIZE = 2000
        while s3_current is not None and db_current is not None:
            s3_hash, s3_size, s3_path = s3_current
            db_hash, db_size = db_current
            if last_prefix != db_hash[:1]:
                last_prefix = db_hash[:1]
                log.info('... hash prefix: %s', last_prefix)
            if s3_hash == db_hash:
                if s3_size != db_size:
                    size_mismatch_total_size += max(s3_size, db_size)
                    size_mismatch_count += 1
                    # size mismatch: retry tasks to fix
                    if retry_tasks:
                        task_results_to_retry.append(db_hash)
                        if len(task_results_to_retry) > TASK_RETRY_BATCH_SIZE:
                            retry_tasks_for_results(task_results_to_retry)
                            task_results_to_retry = []
                s3_current = next(s3_iter, None)
                db_current = next(db_iter, None)
            elif s3_hash < db_hash:
                missing_from_db_count += 1
                missing_from_db_total_size += s3_size
                # tmp and left-over - compare with age and delete from s3
                _delete_from_s3_if_old_enough(s3_path)
                s3_current = next(s3_iter, None)
            else:
                missing_from_s3_count += 1
                missing_from_s3_total_size += db_size
                if retry_tasks:
                    task_results_to_retry.append(db_hash)
                    if len(task_results_to_retry) > TASK_RETRY_BATCH_SIZE:
                        retry_tasks_for_results(task_results_to_retry)
                        task_results_to_retry = []
                db_current = next(db_iter, None)

        while s3_current is not None:
            s3_hash, s3_size, s3_path = s3_current
            missing_from_db_count += 1
            missing_from_db_total_size += s3_size
            _delete_from_s3_if_old_enough(s3_path)
            s3_current = next(s3_iter, None)

        while db_current is not None:
            db_hash, db_size = db_current
            missing_from_s3_count += 1
            missing_from_s3_total_size += db_size
            if retry_tasks:
                task_results_to_retry.append(db_hash)
                if len(task_results_to_retry) > TASK_RETRY_BATCH_SIZE:
                    retry_tasks_for_results(task_results_to_retry)
                    task_results_to_retry = []
            db_current = next(db_iter, None)

    if retry_tasks and len(task_results_to_retry) > 0:
        retry_tasks_for_results(task_results_to_retry)

    if size_mismatch_count:
        log.warning('found DB/S3 size mismatch: count = %s, size = %s',
                    size_mismatch_count, pretty_size(size_mismatch_total_size))

    if missing_from_db_count:
        log.warning('S3 objects missing from DB: count = %s, size = %s',
                    missing_from_db_count, pretty_size(missing_from_db_total_size))

    if missing_from_s3_count:
        log.warning('DB rows missing from S3: count = %s , size = %s',
                    missing_from_s3_count, pretty_size(missing_from_s3_total_size))

    return missing_from_db_count + missing_from_s3_count + size_mismatch_count


def delete_db_blobs(blob_iterator, expected_count):
    """Delete Database and S3 entries for Blobs using this iterator.

    Reports progress as percent.

    Returns a (s3, db) tuple with actual number of items deleted.
    """
    deleted_s3 = 0
    deleted_db = 0
    expected_count += 1

    UPDATE_EVERY = math.ceil(expected_count / 11)

    for i, blob in enumerate(blob_iterator):
        if (i + 1) % UPDATE_EVERY == 0:
            log.info('DELETE %s%%', int(100 * i / expected_count))
        try:
            settings.BLOBS_S3.remove_object(collections.current().name,
                                            models.blob_repo_path(blob.pk))
            deleted_s3 += 1
        except Exception as e:
            log.debug(e)
        blob.delete()
        deleted_db += 1
    return deleted_s3, deleted_db


def check_blobs_orphaned(delete=False, min_age_hours=2):
    """Look for orphaned database Blob entries.

    This is done by automatically fetching all related field named, and
    building a single query that checks for them all.
    This approach is better than manually lisiting fields, since it does not need to be updated.

    Args:
        - delete: if will delete found entries from Database and S3.
        - min_age_hours: objects edited later than this many hours ago are ignored.
    """
    # get all related fields of model Blob.
    # is_relateion=True filters out the actual fields.
    # concrete=False    filters out the Foreign Keys of this Blob pointing to itself (links to parents).
    fields = [f.name for f in models.Blob._meta.get_fields(include_hidden=True)
              if f.is_relation and not f.concrete]
    log.debug('found fields: %s', str(fields))
    query_args = {f + '__isnull': True for f in fields}
    orphaned_blobs = (
        models.Blob.objects
        .filter(**query_args)
        .filter(date_modified__lt=timezone.now() - timedelta(hours=min_age_hours))
        .order_by('pk')
    )

    if not orphaned_blobs.exists():
        return 0
    count = orphaned_blobs.count()
    total_size = orphaned_blobs.aggregate(Sum('size'))['size__sum']
    log.warning('found ORPHANED DB BLOBS: count = %s, size = %s!', count, pretty_size(total_size))
    if delete:
        log.info('starting DELETE of %s Orphaned Blobs from DB...', count)
        s3_deleted, db_deleted = delete_db_blobs(orphaned_blobs, count)
        log.warning('DELETED Orphaned Blobs from DB: S3 count = %s, Database count = %s',
                    s3_deleted, db_deleted)
        count = orphaned_blobs.count()
    return count
