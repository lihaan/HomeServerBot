import logging
from collections import defaultdict


def init_logging(logfile_path):
    logging.basicConfig(
        filename=logfile_path,
        filemode="w",
        format="%(asctime)s, %(levelname)s, %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        level=logging.INFO,
    )


class LogLevelHandler(logging.Handler):
    # Idea from https://stackoverflow.com/a/31142078
    level_messages = None

    def __init__(self, *args, **kwargs):
        super().__init__()
        self.level_messages = defaultdict(list)

    def emit(self, record):
        self.format(record)
        l = record.levelname
        self.level_messages[l].append(record.message)


logLevelCountHandler = LogLevelHandler()


def get_logger(name):
    logger = logging.getLogger(name)
    logger.addHandler(logLevelCountHandler)
    return logger, logLevelCountHandler
