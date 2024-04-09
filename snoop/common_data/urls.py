"""Specific URL routes for a single collection.

Includes routes for the DRF Tags API, and the various document and collection APIs.
"""
from django.urls import path

from . import apps, views
app_name = apps.CommonDataConfig.name
urlpatterns = [
    path('collection-hits', views.get_collection_hits),
    path('sync_nextcloudcollections', views.sync_nextlcoud_collections),
    path('validate_new_collection_name', views.validate_new_collection_name),
    path('remove-nextcloud-collection/<collection_name>', views.remove_nextcloud_collection),
]
