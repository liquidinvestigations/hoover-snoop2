from datetime import timedelta
from django.urls import reverse
from django.contrib import admin
from django.utils.safestring import mark_safe
from django.utils import timezone
from django.template.defaultfilters import truncatechars
from django.urls import path
from django.shortcuts import render
from django.db.models import Sum
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


def get_stats():
    one_minute_ago = timezone.now() - timedelta(minutes=1)

    tasks = models.Task.objects

    tasks_pending = tasks.filter(status=models.Task.STATUS_PENDING)
    tasks_success = tasks.filter(status=models.Task.STATUS_SUCCESS)
    tasks_error = tasks.filter(status=models.Task.STATUS_ERROR)
    tasks_1m = tasks.filter(date_finished__gt=one_minute_ago)

    blobs = models.Blob.objects

    [[db_size]] = raw_sql("select pg_database_size(current_database())")

    return {
        'tasks': {
            'pending': tasks_pending.count(),
            'success': tasks_success.count(),
            'error': tasks_error.count(),
            '1m': tasks_1m.count(),
        },
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
    list_display = ['__str__', 'mime_type', 'mime_encoding']
    list_filter = ['mime_type']
    search_fields = ['sha3_256', 'sha256', 'sha1', 'md5',
                     'magic', 'mime_type', 'mime_encoding']
    readonly_fields = ['sha3_256', 'sha256', 'sha1', 'md5']


class TaskAdmin(admin.ModelAdmin):
    raw_id_fields = ['blob_arg', 'result']
    list_display = ['pk', 'func', 'args', 'created', 'finished',
                    'status', 'details']
    list_filter = ['func', 'status']
    search_fields = ['pk', 'func', 'args', 'error', 'traceback']
    actions = ['retry_selected_tasks']

    LINK_STYLE = {
        'pending': '',
        'success': 'color: green',
        'error': 'color: red',
        'deferred': 'color: grey',
    }

    def created(self, obj):
        return naturaltime(obj.date_created)

    created.admin_order_field = 'date_created'

    def finished(self, obj):
        return naturaltime(obj.date_finished)

    finished.admin_order_field = 'date_finished'

    def details(self, obj):
        if obj.status == models.Task.STATUS_SUCCESS:
            return "✔"

        if obj.status == models.Task.STATUS_ERROR:
            return obj.error

        def link(dep):
            task = dep.prev
            url = reverse('admin:data_task_change', args=[task.pk])
            style = self.LINK_STYLE[task.status]
            return f'<a href="{url}" style="{style}">{dep.name}</a>'

        dep_list = [link(dep) for dep in obj.prev_set.order_by('name')]
        return mark_safe(', '.join(dep_list))

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
            path('shaorma', self.shaorma),
        ]

    def shaorma(self, request):
        return render(request, 'snoop/admin_shaorma.html', get_stats())


site = SnoopAminSite(name='snoopadmin')


site.register(models.Collection)
site.register(models.Directory, DirectoryAdmin)
site.register(models.File, FileAdmin)
site.register(models.Blob, BlobAdmin)
site.register(models.Task, TaskAdmin)
site.register(models.TaskDependency, TaskDependencyAdmin)
site.register(models.Digest, DigestAdmin)
