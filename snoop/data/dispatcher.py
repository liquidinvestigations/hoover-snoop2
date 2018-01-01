from time import sleep
import logging
from . import models
from . import tasks

logger = logging.getLogger(__name__)


def run_dispatcher():
    while True:
        try:
            for collection in models.Collection.objects.all():
                tasks.walk.delay(collection.pk)

            return

            sleep(1)

        except KeyboardInterrupt:
            logger.info('Caught KeyboardInterrupt, exiting')
            return
