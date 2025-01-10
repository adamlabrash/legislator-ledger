import sys
from typing import Any

from loguru import logger as log


def initialize_logger() -> Any:
    log.remove()
    sys.tracebacklimit = 20
    log.add(
        sys.stdout,
        backtrace=True,
        diagnose=True,
        format="<green>{time:DD:HH:mm:ss}</green>|<level>{level}</level>|{file}|{function}|{line}|    <level>{message}</level>",
        level='TRACE',
        colorize=True,
    )
    logger = log.opt(colors=True)

    return logger
