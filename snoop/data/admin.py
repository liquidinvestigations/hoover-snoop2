from django.contrib import admin
from . import models

admin.site.register(models.Collection)
admin.site.register(models.Directory)
admin.site.register(models.File)
admin.site.register(models.Blob)
admin.site.register(models.Task)
admin.site.register(models.TaskDependency)
