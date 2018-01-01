from time import sleep
import logging

logger = logging.getLogger(__name__)


def run_dispatcher():
    while True:
        try:
            sleep(1)

        except KeyboardInterrupt:
            logger.info('Caught KeyboardInterrupt, exiting')
            return
