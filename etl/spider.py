from typing import Iterator
from urllib.parse import unquote
import requests
from bs4 import BeautifulSoup
import warnings
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from csv_loader import write_downloaded_csv_into_aggregate_csv

warnings.filterwarnings('ignore', message='Unverified HTTPS request')


def get_summary_expenditures_html_pages() -> Iterator[str]:
    for quarter in range(1, 5):
        for year in range(2021, 2025):
            url = f'https://www.ourcommons.ca/ProactiveDisclosure/en/members/{year}/{quarter}'
            response = requests.get(url, verify=False)
            response.raise_for_status()
            if response.status_code == 200:
                yield response.text


def extract_csv_download_links(html_pages: Iterator[str]) -> Iterator[str]:
    for html in html_pages:
        soup = BeautifulSoup(html, 'html.parser')

        for link in soup.find_all(attrs={'class': 'light-bold view-report-link'}):
            yield link['href'] + '/csv'


def download_csv_file(csv_download_link: str) -> tuple[str, str] | None:
    try:
        print(f'Downloading {csv_download_link}')
        response = requests.get(f'https://www.ourcommons.ca{csv_download_link}', verify=False)
        if response.status_code == 200:
            headers_partition = unquote(response.headers['Content-Disposition']).split('.csv')[-2].split('_')
            last_name, first_name = headers_partition[-2], headers_partition[-1]
            return response.content.decode('utf-8'), f'{last_name}, {first_name}'

        print(f'Could not download {csv_download_link}', response.status_code)
        return None
    except Exception as e:
        print(f"ERROR HERE: {e}")
        return None


def process_download_links(csv_download_links: Iterator[str]) -> None:
    with ThreadPoolExecutor() as executor:
        res = [executor.submit(download_csv_file, url) for url in csv_download_links]
        wait(res)
        for f in res:
            if result := f.result():
                csv_data, mp_office = result
                write_downloaded_csv_into_aggregate_csv(csv_data, mp_office)


if __name__ == '__main__':
    html_pages = get_summary_expenditures_html_pages()
    csv_download_links = extract_csv_download_links(html_pages)
    process_download_links(csv_download_links)
