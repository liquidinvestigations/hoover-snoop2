import logging
import os

from celery import Celery

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "snoop.defaultsettings")

app = Celery('snoop.data')
app.conf.update(
    worker_log_format="[%(asctime)s: %(name)s %(levelname)s] %(message)s",
)
app.config_from_object('django.conf:settings', namespace='CELERY')
app.conf.task_queue_max_priority = 10
app.conf.task_default_priority = 5
app.autodiscover_tasks()

# from pprint import pprint
# pprint(logging.Logger.manager.loggerDict)
logging.getLogger('celery').setLevel(logging.INFO)
