import json
from datetime import timedelta
from collections import defaultdict
from django.urls import reverse
from django.contrib import admin
from django.conf import settings
from django.utils.safestring import mark_safe
from django.utils import timezone
from django.urls import path
from django.shortcuts import render
from django.db.models import Sum, Count, Avg, F
from django.db import connections
from django.contrib.humanize.templatetags.humanize import naturaltime
from . import models
from . import tasks
from . import collections


def blob_link(blob_pk):
    url = reverse(f'{collections.current().name}:data_blob_change', args=[blob_pk])
    return mark_safe(f'<a href="{url}">{blob_pk[:10]}...{blob_pk[-4:]}</a>')


def raw_sql(query):
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


def get_task_matrix(task_queryset):
    task_matrix = defaultdict(dict)

    task_buckets_query = (
        task_queryset
        .values('func', 'status')
        .annotate(count=Count('*'))
    )
    for bucket in task_buckets_query:
        task_matrix[bucket['func']][bucket['status']] = bucket['count']

    one_minute_ago = timezone.now() - timedelta(minutes=1)
    task_1m_query = (
        task_queryset
        .filter(date_finished__gt=one_minute_ago)
        .values('func')
        .annotate(count=Count('*'))
        .annotate(avg_duration=Avg(F('date_finished') - F('date_started')))
    )
    for bucket in task_1m_query:
        row = task_matrix[bucket['func']]
        pending = row.get('pending', 0)
        count = bucket['count']
        avg_duration = bucket['avg_duration'].total_seconds()
        rate = (count / 60)
        fill = avg_duration * rate * 100
        row['1m'] = count
        row['1m_duration'] = avg_duration
        row['1m_fill'] = f'{fill:.02f}%'
        if pending and rate > 0:
            row['eta'] = timedelta(seconds=int(pending / rate))

    return task_matrix


def get_stats():
    task_matrix = get_task_matrix(models.Task.objects)
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

    return {
        'task_matrix': sorted(task_matrix.items()),
        'counts': {
            'files': models.File.objects.count(),
            'directories': models.Directory.objects.count(),
            'blob_count': blobs.count(),
            'blob_total_size': blobs.aggregate(Sum('size'))['size__sum'],
        },
        'db_size': db_size,
        'error_counts': list(get_error_counts()),
    }


class MultiDBModelAdmin(admin.ModelAdmin):
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


class DirectoryAdmin(MultiDBModelAdmin):
    raw_id_fields = ['parent_directory', 'container_file']


class FileAdmin(MultiDBModelAdmin):
    raw_id_fields = ['parent_directory', 'original', 'blob']
    list_display = ['__str__', 'size', 'mime_type',
                    'original_blob_link', 'blob_link']
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
    list_display = ['__str__', 'mime_type', 'mime_encoding', 'created']
    list_filter = ['mime_type']
    search_fields = ['sha3_256', 'sha256', 'sha1', 'md5',
                     'magic', 'mime_type', 'mime_encoding']
    readonly_fields = ['sha3_256', 'sha256', 'sha1', 'md5', 'created']

    change_form_template = 'snoop/admin_blob_change_form.html'

    def change_view(self, request, object_id, form_url='', extra_context=None):
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
        return naturaltime(obj.date_created)

    def get_preview_content(self, blob):
        if blob.mime_type == 'text/plain':
            with blob.open(encoding=blob.mime_encoding) as f:
                return f.read()

        elif blob.mime_type == 'application/json':
            with blob.open(encoding='utf8') as f:
                return json.dumps(json.load(f), indent=2, sort_keys=True)

        else:
            return ''


class TaskAdmin(MultiDBModelAdmin):
    raw_id_fields = ['blob_arg', 'result']
    list_display = ['pk', 'func', 'args', 'created', 'finished',
                    'status', 'details']
    list_filter = ['func', 'status']
    search_fields = ['pk', 'func', 'args', 'error', 'log']
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
            return obj.error

        return self.dependency_links(obj)

    def retry_selected_tasks(self, request, queryset):
        tasks.retry_tasks(queryset)
        self.message_user(request, f"requeued {queryset.count()} tasks")


class TaskDependencyAdmin(MultiDBModelAdmin):
    raw_id_fields = ['prev', 'next']


class DigestAdmin(MultiDBModelAdmin):
    raw_id_fields = ['blob', 'result']
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


class SnoopAdminSite(admin.AdminSite):
    site_header = "Snoop Mk2"
    index_template = 'snoop/admin_index_default.html'

    def each_context(self, request):
        context = super().each_context(request)
        context['collection_links'] = get_admin_links()
        return context


class CollectionAdminSite(SnoopAdminSite):
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
        with self.collection.set_current():
            context = dict(self.each_context(request))
            context.update(get_stats())
            return render(request, 'snoop/admin_stats.html', context)


def make_collection_admin_site(collection):
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
        site.register(models.OcrSource, MultiDBModelAdmin)
        site.register(models.OcrDocument, MultiDBModelAdmin)
        return site


def get_admin_links():
    global sites
    for name in sorted(sites.keys()):
        yield name, f'/{settings.URL_PREFIX}admin/{name}/'


DEFAULT_ADMIN_NAME = '_default'
sites = {}
for collection in collections.ALL.values():
    sites[collection.name] = make_collection_admin_site(collection)

sites[DEFAULT_ADMIN_NAME] = admin.site
