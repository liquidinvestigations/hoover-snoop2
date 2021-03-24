"""Set-up for the WSGI server.

Nothing interesting to see here; no changes made from Django.
"""

from django.core.wsgi import get_wsgi_application
import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "snoop.defaultsettings")

application = get_wsgi_application()
