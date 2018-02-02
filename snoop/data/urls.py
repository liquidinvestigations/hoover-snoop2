from django.urls import path
from . import views


urlpatterns = [
    path('<name>/json', views.collection),
    path('<name>/feed', views.feed),
    path('<name>/_directory_<int:pk>/json', views.directory),
    path('<name>/<hash>/json', views.document),
    path('<name>/<hash>/raw/<filename>', views.document_download),
    path('<name>/<hash>/locations', views.document_locations),
]
