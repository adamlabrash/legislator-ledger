import scrapy
from scrapy.http import Response
from scrapy.selector import SelectorList

from dotenv import load_dotenv, find_dotenv
import os

load_dotenv(find_dotenv())

# S3 Bucket credentials
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
AWS_BUCKET_NAME = os.environ['AWS_BUCKET_NAME']


class ExpendituresSpider(scrapy.Spider):
    name = "expenditures"
    allowed_domains = ["ourcommons.ca"]

    custom_settings = {
        'ITEM_PIPELINES': {'extraction.expenditures.pipelines.MemberExpenditureSpiderPipeline': 400},
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

    # def __init__(self, execution_date: str):  # execution_date ex -> '2021-03'
    #     self.year = execution_date.split('-')[0]
    #     month = int(execution_date.split('-')[1])
    #     self.quarter = 4 if month < 4 else (month - 1) // 3
    #     self.start_urls = [f'https://www.ourcommons.ca/ProactiveDisclosure/en/members/{self.year}/{self.quarter}']

    def __init__(self):  # To run all at once
        self.start_urls = []
        for year in range(2021, 2025):
            for quarter in range(1, 5):
                self.start_urls.append(f'https://www.ourcommons.ca/ProactiveDisclosure/en/members/{year}/{quarter}')

    def parse(self, response: Response):
        for member in response.xpath('//tr[@class="expenses-main-info"]'):
            fields: SelectorList = member.xpath('.//td')
            download_urls = [url.xpath('./a/@href').get() for url in fields[4:7]]

            member_data = {
                'name': fields[0].xpath('./text()').get('').strip(),
                'constituency': fields[1].xpath('./text()').get('').strip(),
                'caucus': fields[2].xpath('./text()').get('').strip(),
            }
            for download_url in download_urls:
                if download_url:
                    yield response.follow(url=download_url + '/csv', callback=self.parse_csv_page, meta=member_data)

    def parse_csv_page(self, response: Response):
        return {'csv': response.body, 'download_url': response.url} | response.meta
