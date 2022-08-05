""" Check data for orphaned Blobs or discrepancy between S3 and Database.

Optionally delete the Orphaned Database Blobs and S3 objects.
"""

import logging
from datetime import timedelta

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db.models import Sum
from django.utils import timezone


from ... import collections
from ... import models
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

    def handle(self, *args, **options):
        """Runs workers for either collection processing or system tasks."""

        logging_for_management_command()
        collection = options['collection']
        if collection == '__ALL__':
            all_collections = list(collections.ALL.values())
        else:
            all_collections = [collections.ALL[collection]]

        for col in all_collections:
            with col.set_current():
                log.info('\n============\nchecking collection %s\n==============', col.name)
                errors = 0
                errors += check_blobs_orphaned(options['delete_orphaned'], options['min_age_hours'])
                errors += check_blobs_vs_s3()
                if errors > 0:
                    log.error('found %s errors in collection %s', errors, col.name)
                else:
                    log.info('collection %s has no errors', col.name)


def check_blobs_vs_s3():
    """Check for differences between DB and S3 storage mediums.

    Report on:
        - S3 objects not in DB
        - DB objects not in S3
        - documents with differing sizes between DB and S3

    Returns:
        the number of distinct errors
    """
    def s3_hash_size_iter():
        """Generator that returns (sha, size) tuples in order from s3."""
        s3_object_iterator = settings.BLOBS_S3.list_objects(collections.current().name, recursive=True)
        for obj in s3_object_iterator:
            if obj.is_dir:
                continue
            s3_sha3 = obj.object_name.replace('/', '')
            s3_size = obj.size
            yield s3_sha3, s3_size

    def db_hash_size_iter():
        """Generator that returns (sha, size) tuples in order from db."""
        db_iterator = models.Blob.objects.order_by('pk').values('pk', 'size', 'date_modified')
        for vals in db_iterator:
            yield vals['pk'], vals['size']

    s3_iter = s3_hash_size_iter()
    db_iter = db_hash_size_iter()

    size_mismatch_count = 0
    size_mismatch_total_size = 0

    missing_from_s3_count = 0
    missing_from_s3_total_size = 0

    missing_from_db_count = 0
    missing_from_db_total_size = 0

    s3_current = next(s3_iter, None)
    db_current = next(db_iter, None)

    # while both iterators have items, compare the heads.
    # if the head item hashes are equal, check for size difference.
    # if they are different, then save the smaller one, and iterate the respective one.
    while s3_current is not None and db_current is not None:
        s3_hash, s3_size = s3_current
        db_hash, db_size = db_current
        if s3_hash == db_hash:
            if s3_size != db_size:
                size_mismatch_total_size += max(s3_size, db_size)
                size_mismatch_count += 1
            s3_current = next(s3_iter, None)
            db_current = next(db_iter, None)
        elif s3_hash < db_hash:
            missing_from_db_count += 1
            missing_from_db_total_size += s3_size
            s3_current = next(s3_iter, None)
        else:
            missing_from_s3_count += 1
            missing_from_s3_total_size += db_size
            db_current = next(db_iter, None)

    while s3_current is not None:
        missing_from_db_count += 1
        missing_from_db_total_size += s3_size
        s3_current = next(s3_iter, None)

    while db_current is not None:
        missing_from_s3_count += 1
        missing_from_s3_total_size += db_size
        db_current = next(db_iter, None)

    if size_mismatch_count:
        log.warning('found SIZE MISMATCH: count = %s, size = %s',
                    size_mismatch_count, pretty_size(size_mismatch_total_size))

    if missing_from_db_count:
        log.warning('found MISSING FROM DB but present in S3: count = %s, size = %s',
                    missing_from_db_count, pretty_size(missing_from_db_total_size))

    if missing_from_s3_count:
        log.warning('found MISSING FROM S3 but present in DB: count = %s , size = %s',
                    missing_from_s3_count, pretty_size(missing_from_s3_total_size))

    return missing_from_db_count + missing_from_s3_count


def delete_blobs(blob_iterator, expected_count):
    """Delete Database and S3 entries for Blobs using this iterator.

    Reports progress as percent.

    Returns a (s3, db) tuple with actual number of items deleted.
    """
    deleted_s3 = 0
    deleted_db = 0
    expected_count += 1

    UPDATE_EVERY = int(expected_count / 11)

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
    log.warning('found ORPHANED BLOBS: count = %s, size = %s!', count, pretty_size(total_size))
    if delete:
        log.info('starting DELETE of %s Orphaned Blobs...', count)
        s3_deleted, db_deleted = delete_blobs(orphaned_blobs, count)
        log.warning('DELETED Orphaned Blobs: S3 count = %s, Database count = %s',
                    s3_deleted, db_deleted)
        count = orphaned_blobs.count()
    return count
