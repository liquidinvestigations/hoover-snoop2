"""Configuration for Celery.

Logging and Settings for Celery are all handled here.
"""

import logging
import os

from opentelemetry.instrumentation.celery import CeleryInstrumentor
from celery import Celery
from celery.signals import worker_process_init

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "snoop.defaultsettings")


@worker_process_init.connect(weak=False)
def init_celery_tracing(*args, **kwargs):
    CeleryInstrumentor().instrument()


app = Celery('snoop.data')
app.conf.update(
    worker_log_format="[%(asctime)s: %(name)s %(levelname)s] %(message)s",
)
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()

# from pprint import pprint
# pprint(logging.Logger.manager.loggerDict)
logging.getLogger('celery').setLevel(logging.INFO)
