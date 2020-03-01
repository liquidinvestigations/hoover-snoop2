from django.urls import path
from . import views


urlpatterns = [
    path('<collection>/json', views.collection),
    path('<collection>/feed', views.feed),
    path('<collection>/_directory_<int:pk>/json', views.directory),
    path('<collection>/<hash>/json', views.document),
    path('<collection>/<hash>/raw/<filename>', views.document_download),
    path('<collection>/<hash>/ocr/<ocrname>/', views.document_ocr),
    path('<collection>/<hash>/locations', views.document_locations),
]
