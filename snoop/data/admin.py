"""Django Admin definitions.

This defines MultiDBModelAdmin, the root Admin class required to view models from the different databases.

Specialized admin sites for the different tables also live here; we add links, a table with task statistics,
and generally try to restrict editing info that should remain read-only. There are still a million ways to
break or exploit this site from this admin, so we keep it locked under firewall and access it through
tunneling onto the machine.

All the different admin sites are kept in the global dict `sites`. The default admin site is also part of
this dict, under the key "_default". The sites are mapped to URLs in `snoop.data.urls` using this global.
"""

import logging
import math
import json
import time
from datetime import timedelta
from collections import defaultdict
from django.urls import reverse
from django.contrib import admin
from django.conf import settings
from django.utils.safestring import mark_safe
from django.utils import timezone
from django.urls import path
from django.shortcuts import render
from django.db.models import Sum, Count, Avg, F, Min, Max
from django.db import connections
from django.contrib.humanize.templatetags.humanize import naturaltime
from . import models
from . import tasks
from . import collections
from .templatetags import pretty_size


log = logging.getLogger(__name__)


tasks.import_snoop_tasks()


def blob_link(blob_pk):
    """Return markup with link pointing to the Admin Edit page for this Blob."""

    url = reverse(f'{collections.current().name}:data_blob_change', args=[blob_pk])
    return mark_safe(f'<a href="{url}">{blob_pk[:10]}...{blob_pk[-4:]}</a>')


def create_link(model_name, pk, url_description):
    """Creates a link to any other Data entry in the database.

    It uses the auto generated urls from django admin and takes the description
    as input.

    Args:
        model_name: The name of the model that the entry belongs to
        pk: the pk of the object
        url_description: The string that the link should show.
    """

    def escape(htmlstring):
        """Escape HTML tags in admin links."""
        # Stolen from https://stackoverflow.com/a/11550901
        escapes = {
            '\"': '&quot;',
            '\'': '&#39;',
            '<': '&lt;',
            '>': '&gt;',
        }
        # This is done first to prevent escaping other escapes.
        htmlstring = htmlstring.replace('&', '&amp;')
        for seq, esc in escapes.items():
            htmlstring = htmlstring.replace(seq, esc)
        return htmlstring

    url = reverse(f'{collections.current().name}:data_{model_name.lower()}_change', args=[pk])
    url_description = escape(str(url_description))
    return mark_safe(f'<a href="{url}">{url_description}</a>')


def raw_sql(query):
    """Execute SQL string in current collection database."""
    col = collections.current()
    with connections[col.db_alias].cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()


ERROR_STATS_QUERY = (
    "SELECT "
    "    func, "
    "    SUBSTRING(error FOR position('(' IN error) - 1) AS error_type, "
    "    COUNT(*) "
    "FROM data_task "
    "WHERE status = 'error' "
    "GROUP BY func, error_type "
    "ORDER BY count DESC;"
)


def get_task_matrix(task_queryset, prev_matrix={}):
    """Runs expensive database aggregation queries to fetch the Task matrix.

    Included here are: counts aggregated by task function and status; average duration and ETA aggregated by
    function.

    We estimate an ETA for every function type through a naive formula that counts the Tasks remaining and
    divides them by the average duration of Tasks finished in the previous 5 minutes. This is not precise
    for tasks that take more than 5 minutes to finish, so this value fluctuates.

    Data is returned in a JSON-serializable python dict.
    """

    task_matrix = defaultdict(dict)

    for key, func in tasks.task_map.items():
        if func.queue:
            task_matrix[key]['queue'] = func.queue

    task_buckets_query = (
        task_queryset
        .values('func', 'status')
        .annotate(count=Count('*'))
    )
    for bucket in task_buckets_query:
        task_matrix[bucket['func']][bucket['status']] = bucket['count']

    # time frame in the past for which we pull tasks
    mins = 5
    # LIMIT the amount of rows we poll when doing the 4M query
    MAX_ROW_COUNT = 5000
    # Task table row takes about 5K in PG, and blob/data storage fetching does at least 8K of I/O
    SIZE_OVERHEAD = 13 * 2 ** 10
    # Overhead measured for NO-OP tasks; used here to make sure we never divide by 0
    TIME_OVERHEAD = 0.005
    RECENT_SPEED_KEY = str(mins) + 'm_avg_bytes_sec'
    AVG_WORKERS_KEY = str(mins) + 'm_avg_workers'

    task_5m_query = (
        task_queryset
        .filter(date_finished__gt=timezone.now() - timedelta(minutes=mins),
                status=models.Task.STATUS_SUCCESS)[:MAX_ROW_COUNT]
        .values('func')
        .annotate(count=Count('*'))
        .annotate(size=Sum('blob_arg__size'))
        .annotate(start=Min('date_started'))
        .annotate(end=Max('date_finished'))
        .annotate(time=Sum(F('date_finished') - F('date_started')))
    )
    for bucket in task_5m_query:
        row = task_matrix[bucket['func']]
        count = bucket['count']
        real_time = (bucket['end'] - bucket['start']).total_seconds()
        total_time = bucket['time'].total_seconds()
        fill_a = total_time / (real_time + TIME_OVERHEAD)
        fill_b = total_time / (mins * 60)
        fill = round((fill_a + fill_b) / 2, 3)
        # get total system bytes/sec in this period
        size = (bucket['size'] or 0) + SIZE_OVERHEAD * count
        bytes_sec = size / (total_time + TIME_OVERHEAD)
        row[str(mins) + 'm_count'] = count
        row[AVG_WORKERS_KEY] = fill
        row[str(mins) + 'm_avg_duration'] = total_time / count
        row[str(mins) + 'm_avg_size'] = size / count
        row[RECENT_SPEED_KEY] = bytes_sec

    for func in prev_matrix:
        for key in [RECENT_SPEED_KEY, AVG_WORKERS_KEY]:
            old = prev_matrix.get(func, {}).get(key, 0)
            # sometimes garbage appears in the JSON (say, if you edit it manually while working on it)
            if not isinstance(old, (int, float)):
                old = 0
            new = task_matrix.get(func, {}).get(key, 0)
            new = (old + new) / 2
            task_matrix[func][key] = round(new, 2)

    task_success_speed = (
        task_queryset
        .filter(date_finished__isnull=False, status=models.Task.STATUS_SUCCESS)
        .values('func')
        .annotate(size=Avg('blob_arg__size'))
        .annotate(avg_duration=Avg(F('date_finished') - F('date_started')))
        .annotate(total_duration=Sum(F('date_finished') - F('date_started')))
    )
    for bucket in task_success_speed:
        row = task_matrix[bucket['func']]
        row['success_avg_size'] = (bucket['size'] or 0) + SIZE_OVERHEAD
        row['success_avg_duration'] = bucket['avg_duration'].total_seconds() + TIME_OVERHEAD
        row['success_avg_bytes_sec'] = (row['success_avg_size']) / (row['success_avg_duration'])
        row['success_total_duration'] = int(bucket['total_duration'].total_seconds() + TIME_OVERHEAD)
    for func in prev_matrix:
        old = prev_matrix.get(func, {}).get('success_avg_bytes_sec', 0)
        # sometimes garbage appears in the JSON (say, if you edit it manually while working on it)
        if not isinstance(old, (int, float)):
            old = 0
        new = task_matrix.get(func, {}).get('success_avg_bytes_sec', 0)
        if not new and old > 0:
            task_matrix[func]['success_avg_bytes_sec'] = old

    exclude_remaining = [models.Task.STATUS_SUCCESS, models.Task.STATUS_BROKEN, models.Task.STATUS_ERROR]
    task_remaining_total_bytes = (
        task_queryset
        .exclude(status__in=exclude_remaining)
        .values('func')
        .annotate(size=Sum('blob_arg__size'))
        .annotate(count=Count('*'))
    )
    for bucket in task_remaining_total_bytes:
        row = task_matrix[bucket['func']]
        prev_matrix_row = prev_matrix.get(bucket['func'], {})

        row['remaining_size'] = (bucket['size'] or 0) + bucket['count'] * SIZE_OVERHEAD
        speed_success = row.get('success_avg_bytes_sec', 0)
        # the other one is measured over the previous few minutes;
        # average it with this one if it exists
        recent_speed = row.get(RECENT_SPEED_KEY, 0)
        if recent_speed:
            speed = (speed_success + recent_speed) / 2
        else:
            speed = speed_success
        if speed:
            remaining_time = row['remaining_size'] / speed
            eta = remaining_time + row.get('pending', 0) * TIME_OVERHEAD
            # average with simple ETA (count * duration)
            eta_simple = bucket['count'] * row['success_avg_duration']
            eta = (eta + eta_simple) / 2

            # Set a small 0.01 default worker count instead of 0,
            avg_worker_count = row.get(AVG_WORKERS_KEY, 0) + 0.01

            # Divide by avg workers count for this task, to obtain multi-worker ETA.
            eta = eta / avg_worker_count

            # double the estimation, since 3X is too much
            eta = round(eta, 1) * 2

            # Add small time overhead for pending task types,
            # accounting for the 50s refresh interval in queueing new tasks
            # (the dispatcher periodic task) -- the average wait is half that.
            eta += 25

            # if available, average with previous value
            if prev_matrix_row.get('eta', 0) > 1:
                eta = (eta + prev_matrix_row.get('eta', 0)) / 2

            row['eta'] = eta

    return task_matrix


def _get_stats(old_values):
    """Runs expensive database queries to collect all stats for a collection.

    Fetches the Task matrix with `get_task_matrix`, then combines all the different ETA values into a single
    user-friendly ETA text with completed percentage and time to finish. Also computes total counts of the
    different objects (files, directories, de-duplicated documents, blobs) and their total sizes (in the
    database and on disk).

    Data is returned in a JSON-serializable python dict.
    """

    __t0 = time.time()

    def tr(key, value):
        """Render to string in user-friendly format depending on the key"""

        if not value:
            return ''

        if key.endswith('_size'):
            return pretty_size.pretty_size(value)
        if key.endswith('_bytes_sec'):
            return pretty_size.pretty_size(value) + '/s'
        if key.endswith('_duration') or key.endswith('_time') or key == 'eta':
            if isinstance(value, timedelta):
                return pretty_size.pretty_timedelta(value)
            else:
                return pretty_size.pretty_timedelta(timedelta(seconds=value))
        return value

    task_matrix = get_task_matrix(models.Task.objects, old_values.get('_old_task_matrix', {}))
    task2 = []
    task_matrix_header = ['func']
    for row in task_matrix.values():
        for val in row.keys():
            if val not in task_matrix_header:
                task_matrix_header.append(val)
    task_matrix_header = task_matrix_header[:1] + \
        sorted(task_matrix_header[1:],
               key=lambda x: len(x) + ord(x[0]) / 20 + 10 * len(list(1 for y in x if y in '1_')))

    for func in task_matrix.keys():
        row = [func] + [tr(key, task_matrix[func].get(key, None)) for key in task_matrix_header[1:]]
        task2.append(row)

    blobs = models.Blob.objects

    [[db_size]] = raw_sql("select pg_database_size(current_database())")

    def get_error_counts():
        col = collections.current()
        with connections[col.db_alias].cursor() as cursor:
            cursor.execute(ERROR_STATS_QUERY)
            for row in cursor.fetchall():
                yield {
                    'func': row[0],
                    'error_type': row[1],
                    'count': row[2],
                }

    def get_progress_str():
        task_states = defaultdict(int)
        eta = sum((row.get('eta', 0) for row in task_matrix.values()), start=0)
        # if set, round up to exact minutes
        if eta > 1:
            eta = int(math.ceil(eta / 60) * 60)
        eta_str = ', ETA: ' + tr('eta', eta) if eta > 1 else ''
        for row in task_matrix.values():
            for state in row:
                if state in models.Task.ALL_STATUS_CODES:
                    task_states[state] += row[state]
        count_finished = (task_states[models.Task.STATUS_SUCCESS]
                          + task_states[models.Task.STATUS_BROKEN]
                          + task_states[models.Task.STATUS_ERROR])
        count_error = (task_states[models.Task.STATUS_BROKEN]
                       + task_states[models.Task.STATUS_ERROR])
        count_all = sum(task_states.values())
        if count_all == 0:
            return 'empty'
        error_percent = round(100.0 * count_error / count_all, 2)
        finished_percent = round(100.0 * count_finished / count_all, 2)
        error_str = ', %0.2f%% errors' % error_percent if count_error > 0 else ''
        return '%0.1f%% processed%s%s' % (finished_percent, error_str, eta_str)

    stored_blobs = (
        blobs
        .filter(collection_source_key__exact=b'',
                archive_source_key__exact=b'')
    )

    collection_source = (
        blobs
        .exclude(collection_source_key__exact=b'')
    )

    archive_source = (
        blobs
        .exclude(archive_source_key__exact=b'')
    )

    def __get_size(q):
        return q.aggregate(Sum('size'))['size__sum']

    return {
        'task_matrix_header': task_matrix_header,
        'task_matrix': sorted(task2),
        'progress_str': get_progress_str(),
        'counts': {
            'files': models.File.objects.count(),
            'directories': models.Directory.objects.count(),
            'blob_count': blobs.count(),
            'blob_total_size': __get_size(stored_blobs),
            'blob_total_count': stored_blobs.count(),
            'collection_source_size': __get_size(collection_source),
            'collection_source_count': collection_source.count(),
            'archive_source_size': __get_size(archive_source),
            'archive_source_count': archive_source.count(),
        },
        'db_size': db_size,
        'error_counts': list(get_error_counts()),
        '_last_updated': time.time(),
        'stats_collection_time': int(time.time() - __t0) + 1,
        '_old_task_matrix': {k: tr(k, v) for k, v in task_matrix.items()},
    }


def get_stats():
    """This function runs (and caches) expensive collection statistics."""

    col_name_hash = int(hash(collections.current().name))
    if collections.current().process:
        # default stats refresh rate once per 2 min
        REFRESH_AFTER_SEC = 100
        # add pseudorandom 0-40s
        REFRESH_AFTER_SEC += col_name_hash % 40
    else:
        # non-processed collection stats are only pulled once / week
        REFRESH_AFTER_SEC = 604800
        # add a pseudorandom 0-60min based on collection name
        REFRESH_AFTER_SEC += col_name_hash % 3600

    s, _ = models.Statistics.objects.get_or_create(key='stats')
    old_value = s.value
    duration = old_value.get('stats_collection_time', 1) if old_value else 1

    # ensure we don't fill up the worker with a single collection
    REFRESH_AFTER_SEC += duration * 2
    if not old_value or time.time() - old_value.get('_last_updated', 0) > REFRESH_AFTER_SEC:
        s.value = _get_stats(old_value)
    else:
        log.info('skipping stats for collection %s, need to pass %s sec since last one',
                 collections.current().name,
                 REFRESH_AFTER_SEC)
    s.save()
    return s.value


class MultiDBModelAdmin(admin.ModelAdmin):
    """Base class for an Admin that connects to a database different from "default".

    The database is fetched from the thread-local memory using `snoop.data.collections.current()`. See that
    module for details on implementation and limitations.
    """

    allow_delete = False
    allow_change = False

    # A handy constant for the name of the alternate database.
    def __init__(self, *args, **kwargs):
        self.collection = collections.current()
        self.using = self.collection.db_name
        return super().__init__(*args, **kwargs)

    def add_view(self, *args, **kwargs):
        with self.collection.set_current():
            return super().add_view(*args, **kwargs)

    def change_view(self, *args, **kwargs):
        with self.collection.set_current():
            return super().change_view(*args, **kwargs)

    def changelist_view(self, *args, **kwargs):
        with self.collection.set_current():
            return super().changelist_view(*args, **kwargs)

    def delete_view(self, *args, **kwargs):
        with self.collection.set_current():
            return super().delete_view(*args, **kwargs)

    def history_view(self, *args, **kwargs):
        with self.collection.set_current():
            return super().history_view(*args, **kwargs)

    def save_model(self, request, obj, form, change):
        # Tell Django to save objects to the 'other' database.
        obj.save(using=self.using)

    def delete_model(self, request, obj):
        # Tell Django to delete objects from the 'other' database
        obj.delete(using=self.using)

    def get_queryset(self, request):
        # Tell Django to look for objects on the 'other' database.
        return super().get_queryset(request).using(self.using)

    def formfield_for_foreignkey(self, db_field, request, **kwargs):
        # Tell Django to populate ForeignKey widgets using a query
        # on the 'other' database.
        return super().formfield_for_foreignkey(db_field, request, using=self.using, **kwargs)

    def formfield_for_manytomany(self, db_field, request, **kwargs):
        # Tell Django to populate ManyToMany widgets using a query
        # on the 'other' database.
        return super().formfield_for_manytomany(db_field, request, using=self.using, **kwargs)

    def has_delete_permission(self, request, obj=None):
        if not self.allow_delete:
            return False
        # otherwise, check the django permissions
        return super().has_delete_permission(request, obj)

    def has_change_permission(self, request, obj=None):
        if not self.allow_change:
            return False
        # otherwise, check the django permissions
        return super().has_change_permission(request, obj)


class DirectoryAdmin(MultiDBModelAdmin):
    """List and detail views for the folders."""

    raw_id_fields = ['parent_directory', 'container_file']
    readonly_fields = [
        'pk',
        '__str__',
        'parent_directory',
        'container_file',
        'date_modified',
        'date_created',
        'name',
    ]
    list_display = ['pk', '__str__', 'name', 'date_created', 'date_modified']


class EntityAdmin(MultiDBModelAdmin):
    """List and detail views for entities."""

    raw_id_fields = ['type']
    search_fields = ['pk', 'type__pk', 'type__type', 'entity']
    readonly_fields = [
        'pk',
        'entity',
        '_type',
        'parent_link',
        'blacklisted',
        'hit_count',
    ]
    list_display = ['pk', '_type', 'entity',
                    'parent_link', 'blacklisted', 'hit_count']

    def parent_link(self, obj):
        with self.collection.set_current():
            if obj.parent:
                return create_link('entity', obj.parent.pk, obj.parent)
            return '/'

    def hit_count(self, obj):
        with self.collection.set_current():
            return models.EntityHit.objects.filter(entity=obj.pk).count()

    def _type(self, obj):
        with self.collection.set_current():
            return create_link('entitytype', obj.type.pk, str(obj.type))

    allow_delete = True


class EntityHitAdmin(MultiDBModelAdmin):
    """List and detail views for entities."""

    raw_id_fields = ['entity', 'model']
    search_fields = ['pk', 'entity__entity', 'entity__type__type',
                     'digest__blob__pk', 'digest__result__pk']
    readonly_fields = [
        'pk',
        'entity_link',
        'type_link',
        'digest',
        'digest_extra_result',
        'model_link',
        'text_source',
        'start',
        'end',
    ]
    list_display = ['pk', 'entity_link', 'type_link', 'digest',
                    'digest_extra_result', 'model_link',
                    'text_source', 'start', 'end']

    def entity_link(self, obj):
        with self.collection.set_current():
            return create_link('entity', obj.entity.pk, obj.entity.entity)

    def digest_extra_result(self, obj):
        with self.collection.set_current():
            if obj.digest.extra_result:
                return blob_link(obj.digest.extra_result.pk)
    digest_extra_result.admin_order_field = 'digest__extra_result'

    def type_link(self, obj):
        with self.collection.set_current():
            return create_link('entitytype', obj.entity.type.pk, obj.entity.type)

    def model_link(self, obj):
        with self.collection.set_current():
            if obj.model:
                return create_link('languagemodel', obj.model.pk, obj.model)
            return '/'


class EntityTypeAdmin(MultiDBModelAdmin):
    """List and detail views for types."""

    search_fields = ['type']
    readonly_fields = [
        'pk',
        'type',
        'distinct_entity_count',
        'hit_count',
    ]
    list_display = ['pk', 'type', 'distinct_entity_count', 'hit_count']

    def distinct_entity_count(self, obj):
        with self.collection.set_current():
            return models.Entity.objects.filter(type=obj.pk).count()

    def hit_count(self, obj):
        with self.collection.set_current():
            return models.EntityHit.objects.filter(entity__type=obj.pk).count()


class LanguageModelAdmin(MultiDBModelAdmin):
    """List and detail views for entities."""

    search_fields = ['language_code', 'engine', 'model_name']
    readonly_fields = [
        'pk',
        'language_code',
        'engine',
        'model_name',

    ]
    list_display = ['pk', 'language_code', 'engine', 'model_name']


class FileAdmin(MultiDBModelAdmin):
    """List and detail views for the files."""

    raw_id_fields = ['parent_directory', 'original', 'blob']
    list_display = ['__str__', 'size', 'mime_type',
                    'original_blob_link', 'blob_link']
    readonly_fields = [
        'pk', 'parent_directory', 'original', 'original_blob_link',
        'blob', 'blob_link', 'mime_type',
        'ctime', 'mtime', 'size', 'date_created', 'date_modified',
    ]

    search_fields = [
        'original__sha3_256',
        'original__sha256',
        'original__sha1',
        'original__md5',
        'original__magic',
        'original__mime_type',
        'original__mime_encoding',
        'blob__sha3_256',
        'blob__sha256',
        'blob__sha1',
        'blob__md5',
        'blob__magic',
        'blob__mime_type',
        'blob__mime_encoding',
    ]

    def mime_type(self, obj):
        return obj.original.mime_type

    def original_blob_link(self, obj):
        with self.collection.set_current():
            return blob_link(obj.original.pk)

    original_blob_link.short_description = 'original blob'

    def blob_link(self, obj):
        with self.collection.set_current():
            return blob_link(obj.blob.pk)

    blob_link.short_description = 'blob'


class BlobAdmin(MultiDBModelAdmin):
    """List and detail views for the blobs."""

    list_display = ['__str__', 'mime_type', 'mime_encoding', 'created',
                    'storage', 'size',
                    ]
    list_filter = ['mime_type']
    search_fields = ['sha3_256', 'sha256', 'sha1', 'md5',
                     'magic', 'mime_type', 'mime_encoding',
                     'collection_source_key', 'archive_source_key',
                     'archive_source_blob__pk', 'archive_source_blob__md5']
    readonly_fields = ['sha3_256', 'sha256', 'sha1', 'md5', 'created',
                       'size', 'magic', 'mime_type', 'mime_encoding',
                       '_collection_source_key', '_archive_source_key', 'archive_source_blob']

    change_form_template = 'snoop/admin_blob_change_form.html'

    def _collection_source_key(self, obj):
        return obj.collection_source_key.tobytes().decode('utf8', errors='surrogateescape')

    def _archive_source_key(self, obj):
        return obj.collection_source_key.tobytes().decode('utf8', errors='surrogateescape')

    def storage(self, obj):
        return ('collection' if bool(obj.collection_source_key) else (
            'archive' if bool(obj.archive_source_key) else 'blobs'
        ))

    def change_view(self, request, object_id, form_url='', extra_context=None):
        """Optionally fetch and display the actual blob data in the defail view.

        Our detail view is called "change_view" by Django, but we made everything read-only in this admin.
        """

        with self.collection.set_current():
            extra_context = extra_context or {}

            if object_id:
                blob = models.Blob.objects.get(pk=object_id)
                if blob.mime_type in ['text/plain', 'application/json']:
                    extra_context['preview'] = True

                    if request.GET.get('preview'):
                        content = self.get_preview_content(blob)
                        extra_context['preview_content'] = content

            return super().change_view(
                request, object_id, form_url, extra_context=extra_context,
            )

    def created(self, obj):
        """Returns user-friendly string with date created (like "3 months ago")."""
        return naturaltime(obj.date_created)
    created.admin_order_field = 'date_created'

    def size(self, obj):
        return obj.size
    size.admin_order_field = 'size'

    def get_preview_content(self, blob):
        """Returns string with text for Blobs that are JSON or text.

        Used to peek at the Blob data from the Admin without opening a shell.

        Only works for `text/plain` and `application/json` mime types.
        """
        if blob.mime_type == 'text/plain':
            encoding = 'latin1' if blob.mime_encoding == 'binary' else blob.mime_encoding
            with blob.open() as f:
                return f.read().decode(encoding)

        elif blob.mime_type == 'application/json':
            with blob.open() as f:
                return json.dumps(json.load(f), indent=2, sort_keys=True)

        else:
            return ''


class TaskAdmin(MultiDBModelAdmin):
    """List and detail views for the Tasks with Retry action.
    """

    raw_id_fields = ['blob_arg', 'result']
    readonly_fields = ['_blob_arg', '_result', 'pk', 'func', 'args', 'date_created', 'date_started',
                       'date_finished', 'date_modified', 'status', 'details', 'error', 'log',
                       'broken_reason', 'version', 'fail_count',
                       'duration', 'size']
    list_display = ['pk', 'func', 'args', '_blob_arg', '_result', 'created', 'finished',
                    'status', 'details', 'broken_reason', 'duration',
                    'size']
    list_filter = ['func', 'status', 'broken_reason']
    search_fields = ['pk', 'func', 'args', 'error', 'log',
                     'broken_reason', 'blob_arg__pk', 'result__pk']
    actions = ['retry_selected_tasks']

    change_form_template = 'snoop/admin_task_change_form.html'

    LINK_STYLE = {
        'pending': '',
        'success': 'color: green',
        'broken': 'color: orange',
        'error': 'color: red',
        'deferred': 'color: grey',
    }

    def change_view(self, request, object_id, form_url='', extra_context=None):
        """Adds links to the detail page pointing to the Tasks this one depends on."""

        with self.collection.set_current():
            extra_context = extra_context or {}

            if object_id:
                obj = models.Task.objects.get(pk=object_id)
                extra_context['task_dependency_links'] = self.dependency_links(obj)

            return super().change_view(
                request, object_id, form_url, extra_context=extra_context,
            )

    def created(self, obj):
        return naturaltime(obj.date_created)

    created.admin_order_field = 'date_created'

    def finished(self, obj):
        return naturaltime(obj.date_finished)

    finished.admin_order_field = 'date_finished'

    def dependency_links(self, obj):
        def link(dep):
            task = dep.prev
            url = reverse(f'{self.collection.name}:data_task_change', args=[task.pk])
            style = self.LINK_STYLE[task.status]
            return f'<a href="{url}" style="{style}">{dep.name}</a>'

        dep_list = [link(dep) for dep in obj.prev_set.order_by('name')]
        return mark_safe(', '.join(dep_list))

    def details(self, obj):
        pre = f'v{obj.version} '
        if obj.fail_count:
            pre += 'fail=' + str(obj.fail_count) + ' '
        if obj.status == models.Task.STATUS_ERROR:
            return pre + obj.error[:100]
        return mark_safe(pre + self.dependency_links(obj))

    def retry_selected_tasks(self, request, queryset):
        """Action to retry selected tasks."""

        tasks.retry_tasks(queryset)
        self.message_user(request, f"requeued {queryset.count()} tasks")

    def duration(self, obj):
        if obj.date_finished:
            return pretty_size.pretty_timedelta(obj.date_finished - obj.date_started)
        return ''
    duration.admin_order_field = F('date_finished') - F('date_started')

    def size(self, obj):
        return pretty_size.pretty_size(obj.size())
    size.admin_order_field = 'blob_arg__size'

    def _blob_arg(self, obj):
        with self.collection.set_current():
            if obj.blob_arg:
                return blob_link(obj.blob_arg.pk)
    _blob_arg.admin_order_field = 'blob_arg__pk'

    def _result(self, obj):
        with self.collection.set_current():
            if obj.result:
                return blob_link(obj.result.pk)
    _result.admin_order_field = 'result__pk'


class TaskDependencyAdmin(MultiDBModelAdmin):
    """Listing for dependencies between tasks.

    These are skipped when using the TaskAdmin links, but looking at this table may still be interesting.
    """

    raw_id_fields = ['prev', 'next']
    readonly_fields = ['prev', 'next', 'name']
    list_display = ['pk', 'name', 'prev', 'next']
    search_fields = ['prev', 'next', 'name']


class DigestAdmin(MultiDBModelAdmin):
    """Listing and detail views for the Digests.
    """

    raw_id_fields = ['blob', 'result']
    readonly_fields = [
        'blob', 'blob_link', 'result', 'result_link', 'extra_result_link',
        'blob__mime_type', 'date_modified',
    ]
    list_display = ['pk', 'blob__mime_type', 'blob_link',
                    'result_link', 'extra_result_link', 'date_modified']
    search_fields = ['pk', 'blob__pk', 'result__pk']
    # TODO subclass django.contrib.admin.filters.AllValuesFieldListFilter.choices()
    # to set the current collection
    # list_filter = ['blob__mime_type']

    def blob__mime_type(self, obj):
        with self.collection.set_current():
            return obj.blob.mime_type

    def blob_link(self, obj):
        with self.collection.set_current():
            return blob_link(obj.blob.pk)

    def result_link(self, obj):
        with self.collection.set_current():
            return blob_link(obj.result.pk)

    def extra_result_link(self, obj):
        with self.collection.set_current():
            return blob_link(obj.extra_result.pk) if obj.extra_result else None


class DocumentUserTagAdmin(MultiDBModelAdmin):
    """Listing and detail views for the Tags.
    """

    list_display = ['pk', 'user', 'blob', 'tag', 'public', 'date_indexed']
    readonly_fields = [
        'pk', 'user', 'blob', 'tag', 'public', 'date_indexed',
        'date_modified', 'date_created',
    ]
    search_fields = ['pk', 'user', 'blob', 'tag', 'user']


class OcrSourceAdmin(MultiDBModelAdmin):
    """Editable admin views for the OCR Sources.

    These are manually managed through this interface.
    Management commands to rename / edit these also exist.
    """

    allow_delete = True
    allow_change = True


class SnoopAdminSite(admin.AdminSite):
    """Base AdminSite definition, adds list with links to all collection Admins."""

    site_header = "Snoop Mk2"
    index_template = 'snoop/admin_index_default.html'

    def each_context(self, request):
        context = super().each_context(request)
        context['collection_links'] = get_admin_links()
        return context


class CollectionAdminSite(SnoopAdminSite):
    """Admin site that connects to a collection's database.

    Requires that all models linked here be subclasses of MultiDBModelAdmin.
    """

    index_template = 'snoop/admin_index.html'

    def __init__(self, *args, **kwargs):
        self.collection = collections.current()
        return super().__init__(*args, **kwargs)

    def get_urls(self):
        return super().get_urls() + [
            path('stats', self.stats),
        ]

    def admin_view(self, *args, **kwargs):
        with self.collection.set_current():
            return super().admin_view(*args, **kwargs)

    def stats(self, request):
        """Shows tables with statistics for this collection.

        The data is fetched from `snoop.data.models.Statistics` with key = "stats".

        A periodic worker will update this data every minute or so to limit usage and allow monitoring.
        See `snoop.data.tasks.save_stats()` on how this is done.
        """

        with self.collection.set_current():
            context = dict(self.each_context(request))
            # stats, _ = models.Statistics.objects.get_or_create(key='stats')
            context.update(get_stats())
            print(context)
            return render(request, 'snoop/admin_stats.html', context)


def make_collection_admin_site(collection):
    """Registeres all MultiDBModelAdmin classes with a new CollectionAdminSite.
    Args:
        collection: the collection to bind this CollectionAdminSite to.
    """

    with collection.set_current():
        site = CollectionAdminSite(name=collection.name)
        site.site_header = f'collection "{collection.name}"'
        site.index_title = "task stats, logs, results"

        site.register(models.Directory, DirectoryAdmin)
        site.register(models.File, FileAdmin)
        site.register(models.Blob, BlobAdmin)
        site.register(models.Task, TaskAdmin)
        site.register(models.TaskDependency, TaskDependencyAdmin)
        site.register(models.Digest, DigestAdmin)
        site.register(models.DocumentUserTag, DocumentUserTagAdmin)
        site.register(models.OcrSource, OcrSourceAdmin)
        site.register(models.OcrDocument, MultiDBModelAdmin)
        site.register(models.Statistics, MultiDBModelAdmin)
        site.register(models.Entity, EntityAdmin)
        site.register(models.EntityHit, EntityHitAdmin)
        site.register(models.EntityType, EntityTypeAdmin)
        site.register(models.LanguageModel, LanguageModelAdmin)
        return site


def get_admin_links():
    """Yields tuples with admin site name and URL from the global `sites`."""

    global sites
    for name in sorted(sites.keys()):
        yield name, f'/{settings.URL_PREFIX}admin/{name}/'


DEFAULT_ADMIN_NAME = '_default'
sites = {}
for collection in collections.ALL.values():
    sites[collection.name] = make_collection_admin_site(collection)

sites[DEFAULT_ADMIN_NAME] = admin.site
