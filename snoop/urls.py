from django.urls import path, include
from snoop.data import admin

urlpatterns = [
    path('admin/', admin.site.urls),
    path('collections/', include('snoop.data.urls')),
]
