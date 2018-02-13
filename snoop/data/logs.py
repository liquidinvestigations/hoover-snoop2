import logging


def logging_for_management_command(verbosity=0):
    if verbosity > 2:
        level = logging.DEBUG

    else:
        level = logging.INFO

    logging.basicConfig(level=level)
    logging.getLogger().handlers[0].setLevel(level)
