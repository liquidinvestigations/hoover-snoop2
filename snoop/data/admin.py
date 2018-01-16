from django.urls import reverse
from django.contrib import admin
from django.utils.safestring import mark_safe
from . import models


class FileAdmin(admin.ModelAdmin):
    list_display = ['__str__', 'blob_link']

    def blob_link(self, obj):
        url = reverse('admin:data_blob_change', args=[obj.blob.pk])
        return mark_safe(f'<a href="{url}">{obj.blob.pk}</a>')

    blob_link.short_description = 'blob'


class BlobAdmin(admin.ModelAdmin):
    list_display = ['__str__', 'mime_type', 'mime_encoding']
    list_filter = ['mime_type']
    search_fields = ['pk', 'mime_type', 'mime_encoding']


admin.site.register(models.Collection)
admin.site.register(models.Directory)
admin.site.register(models.File, FileAdmin)
admin.site.register(models.Blob, BlobAdmin)
admin.site.register(models.Task)
admin.site.register(models.TaskDependency)
admin.site.register(models.Digest)
