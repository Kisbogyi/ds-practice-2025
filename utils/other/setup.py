import logging
import os
import sys


def initialize_pb_paths():
    pb_path = os.path.abspath(os.path.join(
        os.path.dirname(__file__), '../../utils/pb'))
    for root, dirs, files in os.walk(pb_path):
        sys.path.append(root)


def get_debug_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('<%(levelname)s> %(asctime)s %(name)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger