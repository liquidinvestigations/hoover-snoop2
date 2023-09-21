"""Admin site definitions for common database models"""
from django.contrib import admin
from snoop.common_data.models import CollectionDocumentHit


class CollectionDocumentHitAdmin(admin.ModelAdmin):
    allow_delete = False
    allow_change = False
    # raw_id_fields = []
    list_display = ['pk', '__str__', 'collection_name', 'doc_sha3_256', 'doc_date_added']


admin.site.register(CollectionDocumentHit, CollectionDocumentHitAdmin)
