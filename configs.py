import os
import sys
from typing import Any

from dotenv import find_dotenv, load_dotenv
from logtail import LogtailHandler
from loguru import logger as log

load_dotenv(find_dotenv())

# S3 Bucket credentials
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
AWS_BUCKET_NAME = os.environ['AWS_BUCKET_NAME']


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

    # upload logs to betterStack cloud
    log.add(
        LogtailHandler(source_token="dkddWdG8Jt8cVMzXJyzmLKWg"),
        backtrace=True,
        diagnose=True,
        format="<green>{time:DD:HH:mm:ss}</green>|<level>{level}</level>|{file}|{function}|{line}|    <level>{message}</level>",
        colorize=True,
        level='TRACE',
    )
    logger = log.opt(colors=True)

    return logger
