import logging
from . import celery
from . import models

logger = logging.getLogger(__name__)


@celery.app.task
def walk(collection_pk):
    collection = models.Collection.objects.get(pk=collection_pk)
    logger.info('walking %r', collection)
