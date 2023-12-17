def get_summary_expenditures_html_pages() -> Iterator[str]:
    for quarter in range(1, 5):
        for year in range(2021, 2025):
            url = f'https://www.ourcommons.ca/ProactiveDisclosure/en/members/{year}/{quarter}'
            response = requests.get(url, verify=False)
            response.raise_for_status()
            if response.status_code == 200:
                yield response.text
