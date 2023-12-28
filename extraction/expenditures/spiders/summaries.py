from datetime import date, datetime
from typing import Iterator
from pydantic import BaseModel, Field
import scrapy
from scrapy.http import Response
from scrapy.spiders import CSVFeedSpider
from decimal import Decimal

from expenditures.enums import Caucus

# TODO apply metadata to csv file -> track url and date of update
# TODO get start urls from airflow -> only scrape new data
# TODO download csv file from Members – Summary of Expenditures
# TODO https://www.ourcommons.ca/PublicationSearch/en/?PubType=37 session transcripts etc


# class Role(Enum):
#     MEMBER = 'Member'
#     SPEAKER = 'Speaker'


class OfficerSummary(BaseModel):
    first_name: str
    last_name: str
    honorific_title: str | None
    role: str
    caucus: Caucus | None

    download_url: str
    extracted_at: datetime

    mp_id: str
    quarter: int = Field(ge=1, le=4)
    year: int
    from_date: date | None = None  # TODO make these computed properties?
    end_date: date | None = None
    constituency: str
    salaries: Decimal = Field(decimal_places=2)
    travel: Decimal = Field(decimal_places=2)
    hospitality: Decimal = Field(decimal_places=2)
    contracts: Decimal = Field(decimal_places=2)


class SummariesExpenditureSpider(CSVFeedSpider):
    name = "summaries"
    allowed_domains = ["ourcommons.ca"]

    def __init__(self, *args, **kwargs):
        # TODO get start urls from airflow -> only scrape new data

        # TODO build urls

        # self.start_urls = [kwargs['start_urls']]
        self.start_urls = [
            'https://www.ourcommons.ca/ProactiveDisclosure/en/house-officers/2024/2/summary-expenditures/csv'
        ]

    def parse_row(self, response: Response, row: dict):
        url_parts = response.url.split('/')
        return row | {
            'download_url': response.url,
            'extracted_at': datetime.now(),
            'year': url_parts[-4],
            'quarter': url_parts[-3],
        }
