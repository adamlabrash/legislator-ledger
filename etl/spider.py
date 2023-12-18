from typing import Iterator
from urllib.parse import unquote
import requests
from requests import HTTPError, Response
from requests.structures import CaseInsensitiveDict
from bs4 import BeautifulSoup
import warnings
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from csv_loader import write_downloaded_csv_into_aggregate_csv

warnings.filterwarnings('ignore', message='Unverified HTTPS request')


def get_html_of_commons_webpage(url_path: str) -> Response:
    url = 'https://www.ourcommons.ca' + url_path
    response = requests.get(url, verify=False)
    response.raise_for_status()
    return response


def extract_csv_download_links_from_webpage(html: str) -> Iterator[str]:
    soup = BeautifulSoup(html, 'html.parser')
    for link in soup.find_all(attrs={'class': 'light-bold view-report-link'}):
        yield link['href'] + '/csv'


def get_csv_download_links_from_expenditure_report_pages() -> Iterator[str]:
    for quarter in range(1, 5):
        for year in range(2021, 2025):
            resp = get_html_of_commons_webpage(f'/ProactiveDisclosure/en/members/{year}/{quarter}')
            yield from extract_csv_download_links_from_webpage(resp.text)


def extract_mp_name_from_csv_download_headers(headers: CaseInsensitiveDict[str]) -> str:
    headers_partition = unquote(headers['Content-Disposition']).split('.csv')[-2].split('_')
    last_name, first_name = headers_partition[-2], headers_partition[-1]
    return f'{last_name}, {first_name}'


def download_csv_file(csv_download_link: str) -> tuple[str, str] | None:
    try:
        resp = get_html_of_commons_webpage(csv_download_link)
        if resp.status_code == 200:
            mp_name = extract_mp_name_from_csv_download_headers(resp.headers)
            return resp.content.decode('utf-8'), mp_name
        return None
    except HTTPError as e:
        print(f'ERROR DOWNLOADING CSV: {csv_download_link}', e)
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
    csv_download_links = get_csv_download_links_from_expenditure_report_pages()
    process_download_links(csv_download_links)
