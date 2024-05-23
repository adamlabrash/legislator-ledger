from typing import Any, Generator

import scrapy
from configs import (
    AWS_ACCESS_KEY_ID,
    AWS_BUCKET_NAME,
    AWS_SECRET_ACCESS_KEY,
    initialize_logger,
)
from loguru import logger
from scrapy.crawler import CrawlerProcess
from scrapy.http import Response
from scrapy.selector import SelectorList


class ExpendituresSpider(scrapy.Spider):
    name = "expenditures"
    allowed_domains = ["ourcommons.ca"]

    custom_settings = {
        'LOG_ENABLED': 0,
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 6,
        'AUTOTHROTTLE_START_DELAY': 1,
        'DOWNLOAD_DELAY': 0.3,
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)',
        'ITEM_PIPELINES': {'extraction.pipelines.MemberExpenditureSpiderPipeline': 400},
        'FEEDS': {
            f's3://{AWS_BUCKET_NAME}/%(name)s/%(year)s-%(quarter)s.json': {
                'format': 'json',
                'encoding': 'utf8',
                'store_empty': False,
            }
        },
        'AWS_ACCESS_KEY_ID': AWS_ACCESS_KEY_ID,
        'AWS_SECRET_ACCESS_KEY': AWS_SECRET_ACCESS_KEY,
    }

    def __init__(self) -> None:
        logger.info('Initializing spider...')
        self.start_urls = []
        for year in range(2021, 2025):
            for quarter in range(1, 5):
                self.start_urls.append(
                    f'https://www.ourcommons.ca/ProactiveDisclosure/en/members/{year}/{quarter}'
                )

    def parse(self, response: Response, **kwargs) -> Generator[scrapy.Request, Any, None]:
        '''
        Extracts csv data, constituency, caucus, MP name, and metadata. See ExpenditureItem for full extraction data.
        '''

        logger.info(f'Parsing {response.url}')
        for member in response.xpath('//tr[@class="expenses-main-info"]'):  # type: ignore
            fields: SelectorList = member.xpath('.//td')
            download_urls = [url.xpath('./a/@href').get() for url in fields[4:7]]

            member_data = {
                'name': fields[0].xpath('./text()').get('').strip(),
                'constituency': fields[1].xpath('./text()').get('').strip(),
                'caucus': fields[2].xpath('./text()').get('').strip(),
            }
            for download_url in download_urls:
                if download_url:
                    yield response.follow(
                        url=download_url + '/csv', callback=self.parse_csv_page, meta=member_data
                    )

    def parse_csv_page(self, response: Response) -> Any:
        return {'csv': response.body, 'download_url': response.url} | response.meta


if __name__ == "__main__":
    logger = initialize_logger()
    process = CrawlerProcess()
    process.crawl(ExpendituresSpider)
    process.start()
