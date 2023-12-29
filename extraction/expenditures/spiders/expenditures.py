import csv
from datetime import date, datetime, timedelta
from typing import Iterator
import scrapy
from scrapy.http import Response
from io import StringIO


class ExpendituresSpider(scrapy.Spider):
    name = "expenditures"
    allowed_domains = ["ourcommons.ca"]

    def __init__(self, last_update: date):
        '''
        Quarter 1 = April 1 to June 30
        Quarter 2 = July 1 to September 30
        Quarter 3 = October 1 to December 31
        Quarter 4 = January 1 to March 31
        #TODO get mp ids, move below function to airflow
        '''
        self.start_urls = []
        last_update_quarter = 4 if last_update.month < 4 else (last_update.month - 1) // 3
        last_update_year = last_update.year

        current_quarter = 4 if date.today().month < 4 else (date.today().month - 1) // 3
        current_year = date.today().year

        while last_update_quarter != current_quarter and last_update_year != current_year:
            self.start_urls.append(
                f'https://www.ourcommons.ca/ProactiveDisclosure/en/members/{last_update_year}/{last_update_quarter}'
            )
            if last_update_quarter == 4:
                last_update_quarter = 1
                last_update_year += 1
            else:
                last_update_quarter += 1

    def parse(self, response: Response):
        member_data = (
            response.xpath("//div[h1[contains(text(),'Detailed Travel Expenditures Report')]]")
            .xpath('//h2/text()')[0]
            .extract()
            .split(' - ')
        )
        yield response.follow(url=response.url + '/csv', callback=self.parse_csv, meta={'member_data': member_data})

    def parse_csv(self, response: Response) -> Iterator:
        csv_content = response.body.decode()
        csv_data = csv.reader(StringIO(csv_content), delimiter=',')
        next(csv_data)  # skip title row
        next(csv_data)  # skip header row

        '''
        # TODO use set, error handling
        '''
        claim_travel_events = []
        travel_claim = next(csv_data)

        for row in csv_data:
            if row[0] == travel_claim[0]:
                claim_travel_events.append(row)
            else:
                yield {
                    'claim_row': travel_claim,
                    'travel_event_rows': claim_travel_events,
                    'download_url': response.url,
                } | response.meta

                travel_claim, claim_travel_events = row, []


'''
https://www.ourcommons.ca/ProactiveDisclosure/en/house-administration/2023/4/ExpenditureCategory/csv
https://www.ourcommons.ca/ProactiveDisclosure/en/house-officers/{year}/{quarter}/ExpenditureCategory/csv


https://www.ourcommons.ca/ProactiveDisclosure/en/members/{year}/{quarter}/csv
https://www.ourcommons.ca/ProactiveDisclosure/en/members/{expenditure_category}/{year}/{quarter}/{mp_id}/csv -> get mp_id

https://www.ourcommons.ca/ProactiveDisclosure/en/house-administration/2023/4/csv
https://www.ourcommons.ca/ProactiveDisclosure/en/house-officers/{year}/{quarter}/csv


https://www.ourcommons.ca/Boie/en/reports-and-disclosure -> several things to download here
'''
