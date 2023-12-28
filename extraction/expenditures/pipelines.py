from pydantic import ValidationError
from scrapy.exceptions import DropItem
from expenditures.items import ExpenditureItem


class ExpendituresSpiderPipeline:
    def __init__(self) -> None:
        self.ids_seen = set()

    def process_item(self, item, spider) -> ExpenditureItem:
        try:
            ExpenditureItem.model_validate(item)
        except ValidationError as err:
            # TODO log item and err
            raise DropItem(f"Validation error(s) in {item}: {err}")

        # TODO remove values without claim id

        return item


class OfficerExpenditureSummarySpiderPipeline:
    def process_item(self, item, spider) -> OfficerExpenditureSummary:
        try:
            OfficerExpenditureSummary.model_validate(item)
        except ValidationError as err:
            # TODO log item and err
            raise DropItem(f"Validation error(s) in {item}: {err}")

        return item


'''
Airflow
-> start scrapy
-> wait for scrapy to finish
-> upload csv to snowflake

Scrapy -> extract -> validate -> csv -> snowflake
'''
