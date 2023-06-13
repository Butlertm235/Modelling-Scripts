import logging
logger = logging.getLogger(__name__)

def test():
    logger.warning('Test warning message')
    logger.info('Test info message')
    logger.debug('Test debug message')
    

def multiplier(x, y):
    z = x * y
    logger.info('Calculating %s and %s multiplied together.', x, y)
    return z