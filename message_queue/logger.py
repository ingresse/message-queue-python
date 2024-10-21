import logging
import argparse


def get(name):
    """Get logger

    :param string name: Name of the file

    """
    logger  = logging.getLogger(name)
    logger.setLevel(logging.WARNING)
    handler = logging.StreamHandler()

    handler.setFormatter(
        logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )

    logger.addHandler(handler)

    return logger

