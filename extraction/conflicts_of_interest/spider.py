import scrapy
from scrapy.http import Response
from scrapy.selector import SelectorList


class ConflictsOfInterestSpider(scrapy.Spider):
    name = "conflicts_of_interest"
    # allowed_domains = ["prciec-rpccie.parl.gc.ca"]

    custom_settings = {'ITEM_PIPELINES': {'conflicts_of_interest.pipeline.ConflictsOfInterestPipeline': 400}}

    def __init__(self, last_extraction_date: str | None = '2024-01-5'):  # ex 2024-01-01
        self.start_urls = [
            f'https://prciec-rpccie.parl.gc.ca/EN/PublicRegistries/Pages/PublicRegistry.aspx#Default=%7B%22k%22%3A%22%22%2C%22r%22%3A%5B%7B%22n%22%3A%22OCIECDeclarationDisclosureDate01%22%2C%22t%22%3A%5B%22range({last_extraction_date}%2Cmax)%22%5D%2C%22o%22%3A%22and%22%2C%22k%22%3Afalse%2C%22m%22%3Anull%7D%5D%7D'
        ]

    def parse(self, response: Response):
        for disclosure in response.xpath('//div[@id="WebPartWPQ3"]'):
            import pdb

            pdb.set_trace()
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
                    yield response.follow(url=download_url + '/csv', meta=member_data)
