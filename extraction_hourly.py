from typing import Dict, Tuple
import requests
import logging
import pandas as pd
import time
from datetime import datetime
from dotenv import load_dotenv
import os
import traceback  # For detailed error logging

load_dotenv()  # Load the API key from the .env file

API_KEY = os.environ.get('CRYPTOCOMPARE_API_KEY')
if not API_KEY:
    raise ValueError("API key not found. Please set the CRYPTOCOMPARE_API_KEY in the .env file.")

# API endpoint for hourly data
BASE_URL = 'https://min-api.cryptocompare.com/data/v2/histohour'
MAX_RETRIES = 5  # Maximum number of retries for API requests


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
    Fetches historical hourly data for a specified cryptocurrency from the CryptoCompare API.
    """
    retries = 0  # Initialize retries for each function call
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')  # Default end_date to current date if not provided

    data = []
    toTs = datetime.timestamp(datetime.strptime(end_date, '%Y-%m-%d'))
    start_timestamp = datetime.timestamp(datetime.strptime(start_date, '%Y-%m-%d'))

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

            for record in batch:
                if not validate_data(record):
                    logging.warning(f"Data validation failed for record: {record}")
                else:
                    data.append(record)

            if datetime.fromtimestamp(batch[-1]['time']) < datetime.fromtimestamp(start_timestamp):
                break

            toTs = batch[0]['time']
        except requests.exceptions.RequestException as e:
            logging.error(f"Request exception for {fsym}: {e}")
            if retries >= MAX_RETRIES:
                logging.error(f"Max retries reached for {fsym}. Last attempt failed with: {e}")
                raise
            retries += 1
            logging.info(f"Retrying ({retries}/{MAX_RETRIES}) for {fsym} after failure.")
            time.sleep(10)  # Wait before retrying

        logging.info(f"Data successfully fetched for {fsym}.")
        return pd.DataFrame(data)


def fetch_data(fsym: str, tsym: str, start_date: str, end_date: str = None, limit=2000) -> pd.DataFrame:
    """
    Fetches historical hourly data for a specified cryptocurrency from the CryptoCompare API.
    """
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')  # Default end_date to current date if not provided

    data = []
    toTs = datetime.timestamp(datetime.strptime(end_date, '%Y-%m-%d'))
    start_timestamp = datetime.timestamp(datetime.strptime(start_date, '%Y-%m-%d'))

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

            for record in batch:
                if not validate_data(record):
                    logging.warning(f"Data validation failed for record: {record}")
                else:
                    data.append(record)

            if datetime.fromtimestamp(batch[-1]['time']) < datetime.fromtimestamp(start_timestamp):
                break

            toTs = batch[0]['time']  # Prepare toTs for the next call
        except requests.exceptions.RequestException as e:
            logging.error(f"Request exception for {fsym}: {e}")
            raise

    return pd.DataFrame(data)
