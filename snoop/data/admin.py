from django.contrib import admin
from . import models


class BlobAdmin(admin.ModelAdmin):
    list_display = ['__str__', 'mime_type', 'mime_encoding']
    list_filter = ['mime_type']
    search_fields = ['pk', 'mime_type', 'mime_encoding']


admin.site.register(models.Collection)
admin.site.register(models.Directory)
admin.site.register(models.File)
admin.site.register(models.Blob, BlobAdmin)
admin.site.register(models.Task)
admin.site.register(models.TaskDependency)
admin.site.register(models.Digest)
