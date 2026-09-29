import logging
import sys


def get_logger() -> logging.Logger:
    logger = logging.getLogger("binance_open_interest")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
        logger.addHandler(handler)
    return logger


logger = get_logger()
