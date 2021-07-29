#!/usr/bin/env python3
import logging

logger = logging.getLogger(__name__)


def hello():
    return "hello"


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logger.info('Started')
    logging.info('Finished')

    
