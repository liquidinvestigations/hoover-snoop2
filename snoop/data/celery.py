"""Configuration for Celery.

Logging and Settings for Celery are all handled here.
"""

import logging
import os

from celery import Celery
from celery.signals import \
    worker_process_init, worker_process_shutdown

from snoop.data import tracing

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "snoop.defaultsettings")
logger = logging.getLogger(__name__)
tracer = tracing.Tracer(__name__)


def _clear_s3_mounts():
    from snoop.data.s3 import clear_mounts, refresh_worker_index
    try:
        refresh_worker_index()
        clear_mounts()
    except Exception as e:
        logger.exception(e)


@worker_process_init.connect(weak=False)
def signal_worker_process_init(*args, **kwargs):
    tracing.init_tracing("CELERY")
    _clear_s3_mounts()


@worker_process_shutdown.connect(weak=False)
def signal_worker_process_shutdown(*args, **kwargs):
    tracer.count('process_shutdown')
    _clear_s3_mounts()


app = Celery('snoop.data')
app.conf.update(
    worker_log_format="[%(asctime)s: %(name)s %(levelname)s] %(message)s",
)
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()

# from pprint import pprint
# pprint(logging.Logger.manager.loggerDict)
logging.getLogger('celery').setLevel(logging.ERROR)
