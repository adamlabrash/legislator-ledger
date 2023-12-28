import csv
from datetime import datetime
from typing import Iterator
from urllib.parse import unquote
import scrapy
from scrapy.http import Response
from io import StringIO

# TODO apply metadata to csv file -> track url and date of update
# TODO get start urls from airflow -> only scrape new data
# TODO download csv file from Members – Summary of Expenditures
# TODO https://www.ourcommons.ca/PublicationSearch/en/?PubType=37 session transcripts etc


class ExpendituresSpider(scrapy.Spider):
    name = "expenditures"
    allowed_domains = ["ourcommons.ca"]

    def __init__(self, *args, **kwargs):
        # self.start_urls = [kwargs['start_urls']]

        # TODO pass in IDs and dates from airflow
        self.start_urls = [
            'https://www.ourcommons.ca/ProactiveDisclosure/en/members/travel/2024/2/841d07d2-a1e0-4fd9-afb3-f05260d42001'
        ]

    def parse(self, response: Response):
        member_data = (
            response.xpath("//div[h1[contains(text(),'Detailed Travel Expenditures Report')]]")
            .xpath('//h2/text()')[0]
            .extract()
            .split(' - ')
        )
        name, constituency, caucus = member_data[0], member_data[1], member_data[2]

        url_parts = response.url.split('/')
        meta_data = {
            'download_url': response.url,
            'extracted_at': datetime.now(),
            'category': url_parts[-4],
            'year': url_parts[-3],
            'name': name,
            'constituency': constituency,
            'caucus': caucus,
        }
        yield response.follow(url=response.url + '/csv', callback=self.parse_csv, meta=meta_data)

    def parse_csv(self, response: Response) -> Iterator:
        csv_content = response.body.decode()
        csv_data = csv.reader(StringIO(csv_content), delimiter=',')

        for row in csv_data:
            row = [unquote(cell) for cell in row]

            # TODO do parsing here ****START HERE****
            import pdb

            pdb.set_trace()

        import pdb

        pdb.set_trace()
        yield {}


'''
https://www.ourcommons.ca/ProactiveDisclosure/en/house-administration/2023/4/ExpenditureCategory/csv
https://www.ourcommons.ca/ProactiveDisclosure/en/house-officers/{year}/{quarter}/ExpenditureCategory/csv


https://www.ourcommons.ca/ProactiveDisclosure/en/members/{year}/{quarter}/csv
https://www.ourcommons.ca/ProactiveDisclosure/en/members/{expenditure_category}/{year}/{quarter}/{mp_id}/csv -> get mp_id

https://www.ourcommons.ca/ProactiveDisclosure/en/house-administration/2023/4/csv
https://www.ourcommons.ca/ProactiveDisclosure/en/house-officers/{year}/{quarter}/csv


https://www.ourcommons.ca/Boie/en/reports-and-disclosure -> several things to download here
'''
