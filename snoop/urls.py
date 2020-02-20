from django.urls import path, include

from snoop.data import admin

urlpatterns = [
    path('', admin.redirect_to_admin),
    path('admin/', admin.site.urls),
    path('collections/', include('snoop.data.urls')),
]
