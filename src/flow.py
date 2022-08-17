from extract import Extract
from transform import Transform
from load import Load

from setup_logger import create_logger


def main():
    logger = create_logger(task='Data Pipeline Flow')
    logger.info('Starting tasks run...')

    try:
        extract = Extract()
        extract.run()
    except Exception as error:
        logger.warning('Task Extract Failed. Error: {}'.format(error))

    try:
        transform = Transform()
        transform.run()
    except Exception as error:
        logger.warning('Task Transform Failed. Error: {}'.format(error))

    try:
        load = Load()
        load.run()
    except Exception as error:
        logger.warning('Task Load Failed. Error: {}'.format(error))

    logger.info('Tasks successfully completed...')

if __name__ == "__main__":
    main()
