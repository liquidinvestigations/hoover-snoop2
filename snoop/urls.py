from django.urls import path, include
from django.http import HttpResponseRedirect
from django.conf import settings

from snoop import views
from snoop.data import admin


def redirect_to_admin(request):
    if settings.URL_PREFIX:
        url = '/' + settings.URL_PREFIX + 'admin/'
    else:
        url = '/admin/'
    return HttpResponseRedirect(url)


base_urlpatterns = [
    path('_health', views.health),
    path('', redirect_to_admin),
    path('admin/', admin.site.urls),
    path('collections/', include('snoop.data.urls')),
]

if settings.URL_PREFIX:
    urlpatterns = [path(settings.URL_PREFIX, include(base_urlpatterns))]
else:
    urlpatterns = base_urlpatterns
