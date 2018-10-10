import logging

from celery import Celery

from snoop import set_django_settings

set_django_settings()

app = Celery('snoop.data')
app.conf.update(
    worker_log_format="[%(asctime)s: %(name)s %(levelname)s] %(message)s",
)
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()

# from pprint import pprint
# pprint(logging.Logger.manager.loggerDict)
logging.getLogger('celery').setLevel(logging.WARNING)
