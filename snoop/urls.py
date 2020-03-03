from django.urls import path, include
from django.conf import settings

from snoop import views
from snoop.data import admin

base_urlpatterns = [
    path('_health', views.health),
    path('', admin.redirect_to_admin),
    path('admin/', admin.site.urls),
    path('collections/', include('snoop.data.urls')),
]

if settings.URL_PREFIX:
    urlpatterns = [path(settings.URL_PREFIX, include(base_urlpatterns)]
else:
    urlpatterns = base_urlpatterns
