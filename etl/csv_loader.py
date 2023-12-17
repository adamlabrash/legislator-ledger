from enum import Enum
import csv
from enum import Enum
from urllib.parse import unquote


class ExpenditureCategory(Enum):
    CONTRACTS = 'contract'
    TRAVEL = 'travel'
    HOSPITALITY = 'hospitality'

    @property
    def file_name(self) -> str:
        return f'gov_{self.value}_expenditures.csv'


def determine_expenditure_category_of_csv_file(csv_title_row: str) -> ExpenditureCategory:
    for category in ExpenditureCategory:
        if category.value in csv_title_row.lower():
            return category
    else:
        raise ValueError(f'Could not find expenditure category in {csv_title_row}')


def csv_file_is_initialized(file_name: str) -> bool:
    with open(file_name, 'r') as csvfile:
        return len(csvfile.readlines()) > 0


def write_downloaded_csv_into_aggregate_csv(downloaded_csv_data: str, mp_office: str) -> None:
    try:
        rows = downloaded_csv_data.splitlines()
        expenditure_category = determine_expenditure_category_of_csv_file(csv_title_row=rows[0])

        with open(expenditure_category.file_name, 'a') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter=',')

            # write column headers only if file is not initialized
            if not csv_file_is_initialized(expenditure_category.file_name):
                csvwriter.writerow(
                    [rows[1] + ',MP Office'],
                )

            for row in csv.reader(rows[2:], delimiter=','):
                row = [unquote(cell) for cell in row]
                row += [mp_office]
                csvwriter.writerow(row)
    except Exception as e:
        print(f"ERROR WRITING DATA: {mp_office}", e)
        return
