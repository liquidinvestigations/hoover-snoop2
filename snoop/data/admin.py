"""Django Admin definitions.

This defines MultiDBModelAdmin, the root Admin class required to view models from the different databases.

Specialized admin sites for the different tables also live here; we add links, a table with task statistics,
and generally try to restrict editing info that should remain read-only. There are still a million ways to
break or exploit this site from this admin, so we keep it locked under firewall and access it through
tunneling onto the machine.

All the different admin sites are kept in the global dict `sites`. The default admin site is also part of
this dict, under the key "_default". The sites are mapped to URLs in `snoop.data.urls` using this global.
"""
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


def blob_link(blob_pk):
    """Return markup with link pointing to the Admin Edit page for this Blob."""

    url = reverse(f'{collections.current().name}:data_blob_change', args=[blob_pk])
    return mark_safe(f'<a href="{url}">{blob_pk[:10]}...{blob_pk[-4:]}</a>')


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
    TIME_OVERHEAD = 0.02
    RECENT_SPEED_KEY = str(mins) + 'm_avg_bytes_sec'
    task_5m_query = (
        task_queryset
        .filter(date_finished__gt=timezone.now() - timedelta(minutes=mins))[:MAX_ROW_COUNT]
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
        real_time = (bucket['end'] - bucket['start']).total_seconds() + TIME_OVERHEAD
        total_time = bucket['time'].total_seconds() + TIME_OVERHEAD
        fill = total_time / real_time * 100
        # get total system bytes/sec in this period
        size = (bucket['size'] or 0) + SIZE_OVERHEAD * count
        bytes_sec = size / real_time
        row[str(mins) + 'm_count'] = count
        row[str(mins) + 'm_fill'] = f'{fill:.02f}%'
        row[str(mins) + 'm_avg_duration'] = total_time / count
        row[str(mins) + 'm_avg_size'] = size / count
        row[RECENT_SPEED_KEY] = bytes_sec
    for func in prev_matrix:
        old = prev_matrix.get(func, {}).get(RECENT_SPEED_KEY, 0)
        # sometimes garbage appears in the JSON (say, if you edit it manually while working on it)
        if not isinstance(old, (int, float)):
            old = 0
        new = task_matrix.get(func, {}).get(RECENT_SPEED_KEY, 0)
        if not new:
            task_matrix[func][RECENT_SPEED_KEY] = old
        elif old > 0:
            new = (old + new) / 2
            task_matrix[func][RECENT_SPEED_KEY] = new

    task_success_speed = (
        task_queryset
        .filter(date_finished__isnull=False, status=models.Task.STATUS_SUCCESS)
        .values('func')
        .annotate(size=Avg('blob_arg__size'))
        .annotate(avg_duration=Avg(F('date_finished') - F('date_started')))
    )
    for bucket in task_success_speed:
        row = task_matrix[bucket['func']]
        row['success_avg_size'] = (bucket['size'] or 0) + SIZE_OVERHEAD
        row['success_avg_duration'] = bucket['avg_duration'].total_seconds() + TIME_OVERHEAD
        row['success_avg_bytes_sec'] = (row['success_avg_size']) / (row['success_avg_duration'])
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
        row['remaining_size'] = (bucket['size'] or 0) + bucket['count'] * SIZE_OVERHEAD

        # The "success_avg_bytes_sec" is measured per single thread, so we have to scale it up to average
        # worker count. Parallelization has diminishing returns, so we model this by setting max average
        # worker count to 6
        average_worker_count = min(6, (
            (settings.WORKER_COUNT + settings.SNOOP_MIN_WORKERS + settings.SNOOP_MAX_WORKERS) / 3
        ))
        speed_success = row.get('success_avg_bytes_sec', 0) * average_worker_count
        # the other one is measured over the previous few minutes; average it with this one if it exists
        recent_speed = row.get(RECENT_SPEED_KEY, 0)
        if recent_speed:
            speed = (speed_success + recent_speed) / 2
        else:
            speed = speed_success
        if speed:
            remaining_time = row['remaining_size'] / speed
            eta = remaining_time + row.get('pending', 0) * TIME_OVERHEAD / average_worker_count
            row['eta'] = round(eta, 2)

    return task_matrix


def _get_stats(old_values):
    """Runs expensive database queries to collect all stats for a collection.

    Fetches the Task matrix with `get_task_matrix`, then combines all the different ETA values into a single
    user-friendly ETA text with completed percentage and time to finish. Also computes total counts of the
    different objects (files, directories, de-duplicated documents, blobs) and their total sizes (in the
    database and on disk).

    Data is returned in a JSON-serializable python dict.
    """

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
        eta = sum((row.get('eta', 0) for row in task_matrix.values()), start=0) * 2
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

    return {
        'task_matrix_header': task_matrix_header,
        'task_matrix': sorted(task2),
        'progress_str': get_progress_str(),
        'counts': {
            'files': models.File.objects.count(),
            'directories': models.Directory.objects.count(),
            'blob_count': blobs.count(),
            'blob_total_size': blobs.aggregate(Sum('size'))['size__sum'],
        },
        'db_size': db_size,
        'error_counts': list(get_error_counts()),
        '_last_updated': time.time(),
        '_old_task_matrix': {k: tr(k, v) for k, v in task_matrix.items()},
    }


def get_stats():
    """This function runs (and caches) expensive collection statistics."""

    REFRESH_AFTER_SEC = 10
    s, _ = models.Statistics.objects.get_or_create(key='stats')
    old_value = s.value
    if not old_value or time.time() - old_value.get('_last_updated', 0) > REFRESH_AFTER_SEC:
        s.value = _get_stats(old_value)
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

    list_display = ['__str__', 'mime_type', 'mime_encoding', 'created']
    list_filter = ['mime_type']
    search_fields = ['sha3_256', 'sha256', 'sha1', 'md5',
                     'magic', 'mime_type', 'mime_encoding']
    readonly_fields = ['sha3_256', 'sha256', 'sha1', 'md5', 'created',
                       'size', 'magic', 'mime_type', 'mime_encoding']

    change_form_template = 'snoop/admin_blob_change_form.html'

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

    def get_preview_content(self, blob):
        """Returns string with text for Blobs that are JSON or text.

        Used to peek at the Blob data from the Admin without opening a shell.

        Only works for `text/plain` and `application/json` mime types.
        """
        if blob.mime_type == 'text/plain':
            encoding = 'latin1' if blob.mime_encoding == 'binary' else blob.mime_encoding
            with blob.open(encoding=encoding) as f:
                return f.read()

        elif blob.mime_type == 'application/json':
            with blob.open(encoding='utf8') as f:
                return json.dumps(json.load(f), indent=2, sort_keys=True)

        else:
            return ''


class TaskAdmin(MultiDBModelAdmin):
    """List and detail views for the Tasks with Retry action.
    """

    raw_id_fields = ['blob_arg', 'result']
    readonly_fields = ['blob_arg', 'result', 'pk', 'func', 'args',
                       'date_created', 'date_started', 'date_finished', 'date_modified',
                       'status', 'details', 'error', 'log', 'broken_reason', 'worker']
    list_display = ['pk', 'func', 'args', 'created', 'finished',
                    'status', 'details', 'broken_reason']
    list_filter = ['func', 'status', 'broken_reason']
    search_fields = ['pk', 'func', 'args', 'error', 'log', 'broken_reason']
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
        if obj.status == models.Task.STATUS_ERROR:
            return obj.error[:100]

        return self.dependency_links(obj)

    def retry_selected_tasks(self, request, queryset):
        """Action to retry selected tasks."""

        tasks.retry_tasks(queryset)
        self.message_user(request, f"requeued {queryset.count()} tasks")


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
        'blob', 'blob_link', 'result', 'result_link',
        'blob__mime_type', 'date_modified',
    ]
    list_display = ['pk', 'blob__mime_type', 'blob_link', 'result_link', 'date_modified']
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
