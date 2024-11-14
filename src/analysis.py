import json
from typing import Iterator

from enums import Caucus
from items import ExpenditureItem, HospitalityClaim


def load_expenditures_from_json() -> Iterator[ExpenditureItem]:
    with open('expenditures_test.json', 'r', encoding='utf-8-sig') as file:
        for expenditure in json.load(file):

            yield ExpenditureItem.model_validate(expenditure)


def query_hospitality_claims_example() -> Iterator[ExpenditureItem]:
    for expenditure in load_expenditures_from_json():
        if (
            isinstance(expenditure.claim, HospitalityClaim)
            and (expenditure.caucus is Caucus.LIBERAL or expenditure.caucus is Caucus.GREEN)
            and expenditure.claim.total_cost > 100
        ):
            yield expenditure


def sort_and_rank_hospitality_results(results: Iterator[ExpenditureItem]) -> list[ExpenditureItem]:

    top_3_claims = sorted(
        results,
        key=lambda item: item.claim.total_cost,  # Sort by total_cost of the claims  # type: ignore
        reverse=True,  # Sort in descending order (highest first)
    )[:3]
    return top_3_claims


initial_query_results = list(query_hospitality_claims_example())
print(f'Total num results: {len(list(initial_query_results))}')
print(f'Total sum of costs that fit query: {sum([item.claim.total_cost for item in initial_query_results])}')  # type: ignore


sorted_results = sort_and_rank_hospitality_results(initial_query_results)

for result in sorted_results:
    print()
    print(result.model_dump())
    print()

'''
Chatgpt example prompt:
Paste the code from items.py
I have a python list of ExpenditureItems, how do I print the top 3 hospitality contracts from the liberal party with the highest amount? Make the format easily readable.
'''
