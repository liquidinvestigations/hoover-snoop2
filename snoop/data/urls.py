from django.urls import path
from . import views


urlpatterns = [
    path('<collection>/_directory_<int:pk>/json', views.directory),
    path('<collection>/_file_<int:pk>/json', views.file_view),
    path('<collection>/<hash>/json', views.document),
    path('<collection>/<hash>/raw/<filename>', views.document_download),
    path('<collection>/<hash>/ocr/<ocrname>/', views.document_ocr),
    path('<collection>/<hash>/locations', views.document_locations),
    path('<collection>/json', views.collection),
    path('<collection>/feed', views.feed),
]
