"""Specific URL routes for a single collection.

Includes routes for the DRF Tags API, and the various document and collection APIs.
"""
from django.urls import path

from . import apps, views
app_name = apps.CommonDataConfig.name
urlpatterns = [
    path('collection-hits', views.get_collection_hits),
    path('nextcloudcollections', views.get_nextcloud_collections),
    path('sync_nextcloudcollections', views.sync_nextlcoud_collections),
]
