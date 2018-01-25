from snoop import set_django_settings
set_django_settings()

from django.core.wsgi import get_wsgi_application
application = get_wsgi_application()

from django.conf import settings
from . import defaultsettings
if not settings.DEBUG and settings.SECRET_KEY == defaultsettings.SECRET_KEY:
    raise RuntimeError("Please change the default SECRET_KEY setting")
