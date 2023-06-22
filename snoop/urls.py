"""Root URL routes file.

Points to global health check, admin sites, API documentation generators and the [snoop.data.urls][] URLs.
Also sets global URL prefixes.
"""

from django.urls import path, include, re_path
from django.http import HttpResponseRedirect
from django.conf import settings
from rest_framework import permissions
from drf_yasg.views import get_schema_view
from drf_yasg import openapi

from snoop import views
from snoop.data import admin


def redirect_to_admin(request):
    return HttpResponseRedirect(f'/{settings.URL_PREFIX}admin/_default/')


base_urlpatterns = [
    re_path(r'^_health$', views.health),
    re_path(r'^collections/', include('snoop.data.urls', namespace='data')),
    path(r'drf-api-auth/', include('rest_framework.urls', namespace='rest_framework')),
]

base_urlpatterns += [path(f'admin/{k}/', v.urls) for k, v in admin.sites.items()]
base_urlpatterns += [re_path(r'^$', redirect_to_admin)]

# DRF-YASG
# ========
if settings.DEBUG:
    schema_view = get_schema_view(
        openapi.Info(
            title="Snoop API",
            default_version='v0',
            # description="Liquid API for Tags",
            # contact=openapi.Contact(email="contact@liquiddemo.org"),
            # license=openapi.License(name="MIT License"),
        ),
        public=True,
        permission_classes=[permissions.AllowAny],
        validators=['ssv'],
    )

    schema_urlpatterns = [
        re_path(r'^swagger(?P<format>\.json|\.yaml)$',
                schema_view.without_ui(cache_timeout=0), name='schema-json'),
        re_path(r'^swagger/$', schema_view.with_ui('swagger', cache_timeout=0), name='schema-swagger-ui'),
        re_path(r'^redoc/$', schema_view.with_ui('redoc', cache_timeout=0), name='schema-redoc'),
    ]

    base_urlpatterns += schema_urlpatterns


if settings.DJ_TRACKER_ENABLE:
    from dj_tracker.urls import urlpatterns as dj_tracker_urls

    base_urlpatterns += [
        path("dj-tracker/", include(dj_tracker_urls)),
    ]

if settings.URL_PREFIX:
    urlpatterns = [path(settings.URL_PREFIX, include(base_urlpatterns))]
else:
    urlpatterns = base_urlpatterns
