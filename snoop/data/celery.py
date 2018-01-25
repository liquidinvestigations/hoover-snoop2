import os
from celery import Celery
from snoop import set_django_settings

set_django_settings()

app = Celery('snoop')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()
