
<h1 style="text-align: center;">The Legislator Ledger</h1>

This project tracks and estimates the carbon emissions of Canadian Member of Parliment flights, and aggregates thousands of expenditure reports for individual Members of Parliment into accessible datasets.

Members of Parliment are required to report their travel, hospitality, and third-party contract expenditures as outlined here: https://www.ourcommons.ca/en/open-data#ExpendituresMembers

The expenditure reports of individual members of parliment are organized by quarter and year, with each report available for download on 12000+ seperate webpages - this is prevents meaningful aggregation and analysis of information that should be easily available to the public.

This extraction system aggregates 1,000,000+ expenditures, from 12,000+ seperate expenditure reports into a json file found in /expenditure_data. The dates range from July 1, 2020 to June 30, 2023.


<h2 style="text-align: center;">System Architecture</h2>

<img loading="lazy" src="architecture_diagram.jpeg" />
---
***Note that expenditure reports for Party leaders and House Officers are not in the initial dataset. These expenditures are reported seperately and in a different format, and will added to this dataset in the future***
---

TODO:
-Database uml diagram

-Json schema

-nextjs frontend

-expand dataset
