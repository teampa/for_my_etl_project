# Cryptocurrency ETL project

# Overview
This project comprises an ETL pipeline designed to scrape historical data for the top three cryptocurrencies (Bitcoin, Ethereum, and XRP) from CoinMarketCap, perform data cleansing and transformation, and finally load the processed data into a Google Cloud Storage bucket for further analysis or visualization. The pipeline is orchestrated using Apache Airflow, ensuring that data flows smoothly from extraction through to loading.

# Features
Data Extraction: Scrapes historical price data for Bitcoin, Ethereum, and XRP from CoinMarketCap.
Data Transformation: Cleans and transforms the raw data, calculating daily returns for each cryptocurrency.
Data Loading: Loads the transformed data into a Google Cloud Storage bucket, ready for analysis or visualization.

# Technologies Used
Python: For scripting the ETL logic.
Apache Airflow: For orchestrating the ETL pipeline.
pandas: For data manipulation and transformation.
requests and lxml: For scraping data from the web.
Google Cloud Storage: For storing the processed data.
