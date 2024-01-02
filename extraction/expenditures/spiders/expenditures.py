import scrapy
from scrapy.http import Response
from scrapy.selector import SelectorList


class ExpendituresSpider(scrapy.Spider):
    name = "expenditures"
    allowed_domains = ["ourcommons.ca"]

    def __init__(self, execution_date: str):  # execution_date ex -> '2021-03'
        self.year = execution_date.split('-')[0]
        month = int(execution_date.split('-')[1])
        self.quarter = 4 if month < 4 else (month - 1) // 3
        self.start_urls = [f'https://www.ourcommons.ca/ProactiveDisclosure/en/members/{self.year}/{self.quarter}']

    # def __init__(self):
    #     self.start_urls = []
    #     for year in range(2021, 2025):
    #         for quarter in range(1, 5):
    #             self.start_urls.append(f'https://www.ourcommons.ca/ProactiveDisclosure/en/members/{year}/{quarter}')

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
