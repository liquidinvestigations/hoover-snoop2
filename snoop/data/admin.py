import json
from datetime import timedelta
from collections import defaultdict
from django.urls import reverse
from django.contrib import admin
from django.contrib.auth import models as auth_models
from django.contrib.auth import admin as auth_admin
from django.contrib.admin.views.decorators import staff_member_required
from django.utils.decorators import method_decorator
from django.utils.safestring import mark_safe
from django.utils import timezone
from django.template.defaultfilters import truncatechars
from django.urls import path
from django.shortcuts import render
from django.db.models import Sum, Count
from django.db import connection
from django.contrib.humanize.templatetags.humanize import naturaltime
from . import models
from . import tasks


def blob_link(blob_pk):
    url = reverse('admin:data_blob_change', args=[blob_pk])
    return mark_safe(f'<a href="{url}">{blob_pk[:10]}...{blob_pk[-4:]}</a>')


def raw_sql(query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()


ERROR_STATS_QUERY = (
    "SELECT "
        "func, "
        "SUBSTRING(error FOR position('(' IN error) - 1) AS error_type, "
        "COUNT(*) "
    "FROM data_task "
    "WHERE status = 'error' "
    "GROUP BY func, error_type "
    "ORDER BY count DESC;"
)

def get_stats():
    task_matrix = defaultdict(dict)

    task_buckets_query = (
        models.Task.objects
        .values('func', 'status')
        .annotate(count=Count('*'))
    )
    for bucket in task_buckets_query:
        task_matrix[bucket['func']][bucket['status']] = bucket['count']

    one_minute_ago = timezone.now() - timedelta(minutes=1)
    task_1m_query = (
        models.Task.objects
        .filter(date_finished__gt=one_minute_ago)
        .values('func')
        .annotate(count=Count('*'))
    )
    for bucket in task_1m_query:
        task_matrix[bucket['func']]['1m'] = bucket['count']

    blobs = models.Blob.objects

    [[db_size]] = raw_sql("select pg_database_size(current_database())")

    def get_error_counts():
        with connection.cursor() as cursor:
            cursor.execute(ERROR_STATS_QUERY)
            for row in cursor.fetchall():
                yield {
                    'func': row[0],
                    'error_type': row[1],
                    'count': row[2],
                }

    return {
        'task_matrix': sorted(task_matrix.items()),
        'blobs': {
            'count': blobs.count(),
            'size': blobs.aggregate(Sum('size'))['size__sum'],
        },
        'collections': {
            'files': models.File.objects.count(),
            'directories': models.Directory.objects.count(),
        },
        'database': {
            'size': db_size,
        },
        'error_counts': list(get_error_counts()),
    }


class DirectoryAdmin(admin.ModelAdmin):
    raw_id_fields = ['parent_directory', 'container_file']


class FileAdmin(admin.ModelAdmin):
    raw_id_fields = ['parent_directory', 'original', 'blob']
    list_display = ['__str__', 'size', 'mime_type', 'original_blob_link']
    search_fields = [
        'name',
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
        return blob_link(obj.original.pk)

    original_blob_link.short_description = 'blob'


class BlobAdmin(admin.ModelAdmin):
    list_display = ['__str__', 'mime_type', 'mime_encoding', 'created']
    list_filter = ['mime_type']
    search_fields = ['sha3_256', 'sha256', 'sha1', 'md5',
                     'magic', 'mime_type', 'mime_encoding']
    readonly_fields = ['sha3_256', 'sha256', 'sha1', 'md5', 'created']

    change_form_template = 'snoop/admin_blob_change_form.html'

    def change_view(self, request, object_id, form_url='', extra_context=None):
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


class TaskAdmin(admin.ModelAdmin):
    raw_id_fields = ['blob_arg', 'result']
    list_display = ['pk', 'func', 'args', 'created', 'finished',
                    'status', 'details']
    list_filter = ['func', 'status']
    search_fields = ['pk', 'func', 'args', 'error', 'traceback']
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
            url = reverse('admin:data_task_change', args=[task.pk])
            style = self.LINK_STYLE[task.status]
            return f'<a href="{url}" style="{style}">{dep.name}</a>'

        dep_list = [link(dep) for dep in obj.prev_set.order_by('name')]
        return mark_safe(', '.join(dep_list))


    def details(self, obj):
        if obj.status == models.Task.STATUS_SUCCESS:
            return "✔"

        if obj.status == models.Task.STATUS_ERROR:
            return obj.error

        return self.dependency_links(obj)

    def retry_selected_tasks(self, request, queryset):
        tasks.retry_tasks(queryset)
        self.message_user(request, f"requeued {queryset.count()} tasks")


class TaskDependencyAdmin(admin.ModelAdmin):
    raw_id_fields = ['prev', 'next']


class DigestAdmin(admin.ModelAdmin):
    raw_id_fields = ['blob', 'result']
    list_display = ['pk', 'collection', 'blob__mime_type', 'blob_link',
                    'result_link', 'date_modified']
    list_filter = ['collection__name', 'blob__mime_type']
    search_fields = ['pk', 'collection__pk', 'blob__pk', 'result__pk']

    def blob__mime_type(self, obj):
        return obj.blob.mime_type

    def blob_link(self, obj):
        return blob_link(obj.blob.pk)

    def result_link(self, obj):
        return blob_link(obj.result.pk)


class SnoopAminSite(admin.AdminSite):

    site_header = "Snoop Mk2"

    index_template = 'snoop/admin_index.html'

    def get_urls(self):
        return super().get_urls() + [
            path('stats', self.stats),
        ]

    @method_decorator(staff_member_required)
    def stats(self, request):
        context = dict(self.each_context(request))
        context.update(get_stats())
        return render(request, 'snoop/admin_stats.html', context)


site = SnoopAminSite(name='snoopadmin')


site.register(auth_models.User, auth_admin.UserAdmin)
site.register(auth_models.Group, auth_admin.GroupAdmin)

site.register(models.Collection)
site.register(models.Directory, DirectoryAdmin)
site.register(models.File, FileAdmin)
site.register(models.Blob, BlobAdmin)
site.register(models.Task, TaskAdmin)
site.register(models.TaskDependency, TaskDependencyAdmin)
site.register(models.Digest, DigestAdmin)
site.register(models.OcrSource)
site.register(models.OcrDocument)
