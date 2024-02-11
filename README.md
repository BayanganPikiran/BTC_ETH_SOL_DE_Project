# Bitcoin-Ethereum-Solana Data Engineering Project

## Project Description:
This was my first end-to-end data engineering project.  The theme or end purpose of the project was to examine potential 
relationships between trade fluctuations in Bitcoin, Ethereum, and Solana, both in terms of relative price change and
relative change in trade volume (measured in USD).  The time period addressed was April 20, 2020
through January 3, 2024.  The rationale for these dates was that 1) April 20, 2020 was when Solana first entered exchanges,
so was the first date we could track all three coins, and 2) January 3 was the date the initial extraction was executed. 
Both daily and hourly data were incorporated into the study.

The project includes the following steps:
- Extraction of historical data of respective cryptocurrencies from cryptocompare.com's API.
  - An account can be registered and relevant data extracted for free.
  - Data was fetched via the Requests module, returned as a Pandas dataframe, and then turned into a CSV.
- Transformation of the CSV files into a structure most suitable for forthcoming analysis using Pandas tools.
- Creation of a PostgreSQL database and tables.
- Loading of transformed CSVs into the PostgreSQL database.
  - Psycopg2 was used to connect to the database.
  - Sensitive database information and file paths were kept in a .env file and accessed with the OS library.
  - A dry run is performed prior to the actual loading of the data.
- Consistent logging throughout the three aforementioned processes.
- Explicit error handling.
- The creation of backup files and a backup directory.
- Examination of loaded data in PostgreSQL database.
- An analysis of the data in Jupyter Notebook.  This includes:
  - Examination of the datasets.
  - Adding % change columns to the datasets.
  - Calculating Pearson Correlation.
  - Plotting heatmaps.
  - Generating scatter plots mapping relationships between relevant cross-coin variables.
  - Conducting and creating legible graphics for time series decompositions.
  - Performing lead-lag correlation analysis.
  - Performing Granger causality tests.
  - Providing reflections on the given analysis.

I used ChatGPT4 throughout the process.  I find it particularly useful in suggesting ways to produce more thorough scripts,
in writing concise code for producing graphs and plots within my intended scope, and for writing documentation.  It was
excellent practice in what will no doubt be an indispensable tool in future projects.  Perhaps I will pick back up on the project
in the future and complete the cloud work.

## Directions

There are six primary scripts for the ETL pipeline element of the project:
- extraction.py and extraction_hourly.py
- transformation.py and transformation_hourly.py
- loading.py and loading_hourly.py

In order to run this one will need to set up an account with cryptocompare.com, set up database tables and add 
the relevant database configuration constants.  Additionally, and path constants must be added to the loading script.
The requirements.txt and requirements_notebook.txt list all the dependencies needed for both the Python scripts and the
Jupyter Notebook.

## Suggestions and General Contact
If you wish to contact me in regards to this project or any of my other humble projects, my contact details are in my
Github profile.  I look forward to hearing from you.
