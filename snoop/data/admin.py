from django.urls import reverse
from django.contrib import admin
from django.utils.safestring import mark_safe
from django.template.defaultfilters import truncatechars
from . import models


def short(blob_pk):
    return f"{blob_pk[:10]}...{blob_pk[-4:]}"


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
        url = reverse('admin:data_blob_change', args=[obj.blob.pk])
        return mark_safe(f'<a href="{url}">{obj.blob.pk}</a>')

    blob_link.short_description = 'blob'


class BlobAdmin(admin.ModelAdmin):
    list_display = ['__str__', 'mime_type', 'mime_encoding']
    list_filter = ['mime_type']
    search_fields = ['pk', 'mime_type', 'mime_encoding']


class TaskAdmin(admin.ModelAdmin):
    list_display = ['pk', 'func', 'args', 'status']
    list_filter = ['func', 'status']
    search_fields = ['pk', 'func', 'args']


class DigestAdmin(admin.ModelAdmin):
    list_display = ['pk', 'collection', 'blob__mime_type', 'blob_link',
                    'result_link']
    list_filter = ['collection__name', 'blob__mime_type']
    search_fields = ['pk', 'collection__pk', 'blob__pk', 'result__pk']

    def blob__mime_type(self, obj):
        return obj.blob.mime_type

    def blob_link(self, obj):
        url = reverse('admin:data_blob_change', args=[obj.blob.pk])
        return mark_safe(f'<a href="{url}">{short(obj.blob.pk)}</a>')

    def result_link(self, obj):
        url = reverse('admin:data_blob_change', args=[obj.result.pk])
        return mark_safe(f'<a href="{url}">{short(obj.result.pk)}</a>')


admin.site.register(models.Collection)
admin.site.register(models.Directory)
admin.site.register(models.File, FileAdmin)
admin.site.register(models.Blob, BlobAdmin)
admin.site.register(models.Task, TaskAdmin)
admin.site.register(models.TaskDependency)
admin.site.register(models.Digest, DigestAdmin)
