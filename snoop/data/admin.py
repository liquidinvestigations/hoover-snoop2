from datetime import timedelta
from django.urls import reverse
from django.contrib import admin
from django.utils.safestring import mark_safe
from django.utils import timezone
from django.template.defaultfilters import truncatechars
from django.urls import path
from django.shortcuts import render
from django.db.models import Sum
from . import models
from . import tasks


def blob_link(blob_pk):
    url = reverse('admin:data_blob_change', args=[blob_pk])
    return mark_safe(f'<a href="{url}">{blob_pk[:10]}...{blob_pk[-4:]}</a>')


def get_stats():
    one_minute_ago = timezone.now() - timedelta(minutes=1)

    tasks = models.Task.objects

    tasks_pending = tasks.filter(status=models.Task.STATUS_PENDING)
    tasks_success = tasks.filter(status=models.Task.STATUS_SUCCESS)
    tasks_error = tasks.filter(status=models.Task.STATUS_ERROR)
    tasks_1m = tasks.filter(date_finished__gt=one_minute_ago)

    blobs = models.Blob.objects

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
    }


class FileAdmin(admin.ModelAdmin):
    list_display = ['__str__', 'size', 'mime_type', 'blob_link']
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
        return obj.blob.mime_type

    def blob_link(self, obj):
        return blob_link(obj.blob.pk)

    blob_link.short_description = 'blob'


class BlobAdmin(admin.ModelAdmin):
    list_display = ['__str__', 'mime_type', 'mime_encoding']
    list_filter = ['mime_type']
    search_fields = ['sha3_256', 'sha256', 'sha1', 'md5',
                     'magic', 'mime_type', 'mime_encoding']
    readonly_fields = ['sha3_256', 'sha256', 'sha1', 'md5']


class TaskAdmin(admin.ModelAdmin):
    list_display = ['pk', 'func', 'args', 'status', 'deps']
    list_filter = ['func', 'status']
    search_fields = ['pk', 'func', 'args']
    actions = ['retry_selected_tasks']

    LINK_STYLE = {
        'pending': '',
        'success': 'color: green',
        'error': 'color: red',
        'deferred': 'color: grey',
    }

    def deps(self, obj):
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


class DigestAdmin(admin.ModelAdmin):
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
site.register(models.Directory)
site.register(models.File, FileAdmin)
site.register(models.Blob, BlobAdmin)
site.register(models.Task, TaskAdmin)
site.register(models.TaskDependency)
site.register(models.Digest, DigestAdmin)
