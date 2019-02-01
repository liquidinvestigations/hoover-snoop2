from snoop import set_django_settings
set_django_settings()

from django.core.wsgi import get_wsgi_application
application = get_wsgi_application()

from whitenoise.django import DjangoWhiteNoise
application = DjangoWhiteNoise(application)

from django.conf import settings
from . import defaultsettings
if settings.SECRET_KEY == defaultsettings._default_secret_key:
    if not settings.DEBUG:
        raise RuntimeError("Please change the default SECRET_KEY setting")
