from django.urls import path, include
from rest_framework.routers import SimpleRouter
from . import views, apps

app_name = apps.DataConfig.name
tags_router = SimpleRouter(trailing_slash=False)
tags_router.register('/?', views.TagViewSet, basename='documentusertag')

urlpatterns = [
    path('<collection>/feed', views.feed),
    path('<collection>/json', views.collection),

    path('<collection>/_directory_<int:pk>/json', views.directory),
    path('<collection>/_file_<int:pk>/json', views.file_view),

    path('<collection>/<hash>/json', views.document),
    path('<collection>/<hash>/locations', views.document_locations),
    path('<collection>/<hash>/ocr/<ocrname>', views.document_ocr),
    path('<collection>/<hash>/raw/<filename>', views.document_download),

    path('<collection>/<hash>/tags/<username>', include(tags_router.urls)),
]
