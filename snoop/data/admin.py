from django.urls import reverse
from django.contrib import admin
from django.utils.safestring import mark_safe
from . import models


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


admin.site.register(models.Collection)
admin.site.register(models.Directory)
admin.site.register(models.File, FileAdmin)
admin.site.register(models.Blob, BlobAdmin)
admin.site.register(models.Task, TaskAdmin)
admin.site.register(models.TaskDependency)
admin.site.register(models.Digest)
