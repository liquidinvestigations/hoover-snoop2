"""Admin site definitions for common database models"""
from django.contrib import admin
from snoop.common_data.models import CollectionDocumentHit


class CollectionDocumentHitAdmin(admin.ModelAdmin):
    pass


admin.site.register(CollectionDocumentHit, CollectionDocumentHitAdmin)
