from snoop import set_django_settings
set_django_settings()

from django.core.wsgi import get_wsgi_application
application = get_wsgi_application()

from django.conf import settings
from .defaultsettings import default_secret_key
if settings.SECRET_KEY == default_secret_key and not settings.DEBUG:
    raise RuntimeError("Please change the default SECRET_KEY setting")
