"""Specific URL routes for a single collection.

Includes routes for the DRF Tags API, and the various document and collection APIs.
"""
from django.urls import include, path
from graphene_django.views import GraphQLView
from rest_framework.routers import SimpleRouter

from . import apps, views

app_name = apps.DataConfig.name
tags_router = SimpleRouter(trailing_slash=False)
tags_router.register('/?', views.TagViewSet, basename='documentusertag')

urlpatterns = [
    path('<collection>/feed', views.feed),
    path('<collection>/json', views.collection),
    path('<collection>/write_settings', views.write_settings),

    path('<collection>/_directory_<int:pk>/json', views.directory),
    path('<collection>/_file_<int:pk>/json', views.file_view),

    path('<collection>/<hash>/json', views.document),
    path('<collection>/<hash>/locations', views.document_locations),
    path('<collection>/<hash>/ocr/<ocrname>', views.document_ocr),
    path('<collection>/<hash>/raw/<filename>', views.document_download),

    path('<collection>/<hash>/tags/<username>/<uuid>', include(tags_router.urls)),
    path('<collection>/<hash>/thumbnail/<size>.jpg', views.thumbnail),
    path('<collection>/<hash>/pdf-preview', views.pdf_preview),
    path('<collection>/graphql', views.collection_view(GraphQLView.as_view(graphiql=True))),

    path('<collection>/<int:directory_pk>/rescan', views.rescan_directory),
    path('<collection>/<int:directory_pk>/path', views.get_path),
    path('<collection>/<int:directory_pk>/<filename>/exists', views.file_exists),
    path('<collection>/<hash>/processing_status', views.processing_status),
]
