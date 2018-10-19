from django.urls import path
from . import views


urlpatterns = [
    path('json', views.collection),
    path('feed', views.feed),
    path('_directory_<int:pk>/json', views.directory),
    path('<hash>/json', views.document),
    path('<hash>/raw/<filename>', views.document_download),
    path('<hash>/ocr/<ocrname>/', views.document_ocr),
    path('<hash>/locations', views.document_locations),
]
