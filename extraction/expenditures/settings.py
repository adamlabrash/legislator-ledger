from dotenv import load_dotenv, find_dotenv
import os

load_dotenv(find_dotenv())

# S3 Bucket credentials
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
AWS_BUCKET_NAME = os.environ['AWS_BUCKET_NAME']

BOT_NAME = "expenditures_spider"
SPIDER_MODULES = ["expenditures.spiders"]
NEWSPIDER_MODULE = "expenditures.spiders"
LOG_LEVEL = "ERROR"


FEEDS = {
    f's3://{AWS_BUCKET_NAME}/%(name)s/%(year)s-%(quarter)s.json': {
        'format': 'json',
        'encoding': 'utf8',
        'store_empty': False,
    }
}
ROBOTSTXT_OBEY = True
ITEM_PIPELINES = {
    "expenditures.pipelines.MemberExpenditureSpiderPipeline": 300,
}

AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 3
AUTOTHROTTLE_TARGET_CONCURRENCY = 2.0

# Set settings whose default value is deprecated to a future-proof value
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
