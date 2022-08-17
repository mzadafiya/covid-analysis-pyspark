
###################
# SETUP LOGGER
###################

import logging
import os


def create_logger(task):
    """Create logger

    Args:
        task (string): task name

    Returns:
        logger: logger object

    # example usage
    from logger import create_logger
    logger = create_logger()
    logger.info('info message')
    """

    # create logger
    logger = logging.getLogger(task)
    # set log level for all handlers to debug
    logger.setLevel(logging.DEBUG)

    # log file name
    log_file = "./logs/flow.log"
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    # create file handler and set level to debug
    # best for development or debugging
    fileHandler = logging.FileHandler(log_file)
    fileHandler.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # add formatter to fh
    fileHandler.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(fileHandler)

    return logger
