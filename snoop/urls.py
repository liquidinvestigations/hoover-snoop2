from django.urls import path, include
from django.http import HttpResponseRedirect
from django.conf import settings

from snoop import views
from snoop.data import admin


def redirect_to_admin(request):
    return HttpResponseRedirect(f'/{settings.URL_PREFIX}admin/_default/')


base_urlpatterns = [
    path('_health', views.health),
    path('', redirect_to_admin),
    path('collections/', include('snoop.data.urls')),
] + [path(f'admin/{k}/', v.urls) for k, v in admin.sites.items()]

if settings.URL_PREFIX:
    urlpatterns = [path(settings.URL_PREFIX, include(base_urlpatterns))]
else:
    urlpatterns = base_urlpatterns
