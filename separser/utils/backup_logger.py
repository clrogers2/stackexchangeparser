import logging

logger = logging.getLogger("StackExchangeParser")
syslog = logging.FileHandler(filename='./separse.log', encoding='utf-8')
formatter = logging.Formatter('%(asctime)s %(name)s - %(levelname)s:%(message)s')
syslog.setFormatter(formatter)
logger.setLevel("INFO")
logger.addHandler(syslog)


def log(message):
    logger.info(message)