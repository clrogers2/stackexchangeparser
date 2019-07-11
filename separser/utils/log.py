import logging
from pathlib import Path


class Log(object):
    def __init__(self, log_dir=None):
        self.name = "StackExchangeParser"
        if not log_dir:
            self.log_dir = Path.home().joinpath('logs/').as_posix()

        loggers = logging.Logger.manager.loggerDict
        if self.name in loggers.keys():
            self._logger = logging.getLogger(self.name)
            if not self._logger.handlers:
                self._logger = self.init_logger(self.name)
        else:
            self._logger = self.init_logger(self.name)

    def init_logger(self, name):
        logger = logging.getLogger(name)
        syslog = logging.FileHandler(filename=self.log_dir + 'separse.log', encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s %(name)s - %(levelname)s:%(message)s')
        syslog.setFormatter(formatter)
        logger.setLevel('INFO')
        logger.addHandler(syslog)
        return logger

    # TODO refactor into class that has various verbosity levels
    def _log(self, message, *args, **kwargs):
        logger = self._logger
        logger.info(message)
        [logger.debug(arg) for arg in args]
        [logger.debug(kwarg) for kwarg in kwargs]

    def __call__(self, message, *args, **kwargs):
        return self._log(message, *args, **kwargs)
