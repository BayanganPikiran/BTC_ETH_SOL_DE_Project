"""Crypto Data Ingestion Script

This script is used for ingesting historical cryptocurrency data from the CryptoCompare API.
It fetches daily historical data for Bitcoin (BTC), Ethereum (ETH), and Solana (SOL) from a
specified start date and saves the data into CSV files for each cryptocurrency.

The script uses environment variables for API key management and includes detailed logging
for monitoring and debugging. It handles API pagination, ensures data integrity through validation,
and implements retry logic for robust data fetching.

Usage:
    Run the script directly to fetch and save the historical data for the specified cryptocurrencies.
    The start date for data fetching can be modified in the '__main__' section of the script.

Environment Variables:
    CRYPTOCOMPARE_API_KEY: API key for accessing the CryptoCompare API.

Author: Andre La Flamme
Date: January 2, 2024
"""

from typing import Dict, Tuple
import requests
import logging
import pandas as pd
import time
from datetime import datetime
from dotenv import load_dotenv
import os
import traceback  # For detailed error logging

# Constants
DEBUG_MODE = os.getenv('DEBUG_MODE', 'False').lower() == 'true'
LOG_LEVEL = logging.DEBUG if DEBUG_MODE else logging.INFO
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
LOG_FILE = 'extraction_daily.log'

BASE_URL = 'https://min-api.cryptocompare.com/data/v2/histoday'
MAX_RETRIES = 3  # Maximum number of retries for API requests

load_dotenv()  # Load the API key from the .env file

API_KEY = os.environ.get('CRYPTOCOMPARE_API_KEY')
if not API_KEY:
    raise ValueError("API key not found. Please set the CRYPTOCOMPARE_API_KEY in the .env file.")

# Setup logging
logging.basicConfig(filename=LOG_FILE, level=LOG_LEVEL, format=LOG_FORMAT)




def validate_data(data: Dict) -> bool:
    """
    Validates whether the required keys are present in a given data record.

    This function checks if each of the required keys for a valid cryptocurrency data record
    is present in the provided data dictionary. It's used to ensure the integrity and completeness
    of the data fetched from the API.

    Parameters:
    - data (dict): The data record to validate, typically a dictionary representing a single
      cryptocurrency data point (e.g., daily historical data for BTC).

    Returns:
    - bool: Returns True if all required keys are present in the data, False otherwise.
    """
    required_keys = ['time', 'high', 'low', 'open', 'close', 'volumefrom', 'volumeto']
    return all(key in data for key in required_keys)


def fetch_data(fsym: str, tsym: str, start_date: str, end_date: str = None, limit=2000) -> pd.DataFrame:
    """
    Fetches historical data for a specified cryptocurrency from the CryptoCompare API.

    This function handles the fetching of daily historical data for a given cryptocurrency
    symbol (fsym) against a target currency symbol (tsym). It includes pagination to handle
    API limits and a retry mechanism for handling request failures.

    Parameters:
    - fsym (str): Symbol of the cryptocurrency (e.g., 'BTC').
    - tsym (str): Symbol of the target currency to convert into (e.g., 'USD').
    - start_date (str): The start date for fetching data in 'YYYY-MM-DD' format.
    - end_date (str, optional): The end date for fetching data in 'YYYY-MM-DD' format. Defaults to the current date.
    - limit (int, optional): The number of data points to return per request. Defaults to 2000.

    Returns:
    - DataFrame: A pandas DataFrame containing the historical data.
    """
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')  # Default end_date to current date if not provided

    data = []
    toTs = datetime.timestamp(datetime.strptime(end_date, '%Y-%m-%d'))
    retries = 0

    while True:
        params = {
            'fsym': fsym,
            'tsym': tsym,
            'limit': limit,
            'toTs': toTs,
            'api_key': API_KEY
        }

        try:
            response = requests.get(BASE_URL, params=params)
            response.raise_for_status()
            batch = response.json()['Data']['Data']

            # Validate each record in the batch
            for record in batch:
                if not validate_data(record):
                    logging.warning(f"Data validation failed for record: {record}")
                else:
                    data.append(record)

            if datetime.fromtimestamp(batch[-1]['time']) < datetime.strptime(start_date, '%Y-%m-%d'):
                break

            toTs = batch[0]['time']  # Prepare toTs for the next call
            retries = 0
        except requests.exceptions.RequestException as e:
            retries += 1
            logging.error(f"Request exception for {fsym}: {e}")
            if retries > MAX_RETRIES:
                raise
            time.sleep(10)  # Wait before retrying

    return pd.DataFrame(data)


def fetch_all_crypto_data(start_date: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Fetches historical data for Bitcoin (BTC), Ethereum (ETH), and Solana (SOL) from the CryptoCompare API.

    This function calls the 'fetch_data' function for each of the three cryptocurrencies (BTC, ETH, and SOL),
    using a common start date. It returns the historical data for each cryptocurrency as separate pandas DataFrames.

    Parameters:
    - start_date (str): The start date for fetching data in 'YYYY-MM-DD' format.

    Returns:
    - Tuple of DataFrames: A tuple containing three pandas DataFrames with historical data for BTC, ETH, and SOL.
    """
    btc_data = fetch_data('BTC', 'USD', start_date)
    eth_data = fetch_data('ETH', 'USD', start_date)
    sol_data = fetch_data('SOL', 'USD', start_date)

    return btc_data, eth_data, sol_data


if __name__ == "__main__":
    original_start_date = '2020-03-24'

    try:
        btc_data, eth_data, sol_data = fetch_all_crypto_data(original_start_date)
        print("Data fetched successfully for BTC, ETH, and SOL.")

        btc_data.to_csv('btc_data.csv', index=False)
        eth_data.to_csv('eth_data.csv', index=False)
        sol_data.to_csv('sol_data.csv', index=False)
        print("\nData exported to CSV files in the root directory.")

    except Exception as e:
        logging.error(f"Unhandled exception: {e}")
        logging.error(traceback.format_exc())
        print(f"Error occurred: {e}")
