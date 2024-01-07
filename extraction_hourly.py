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


def validate_hourly_data(data: Dict) -> bool:
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


def fetch_hourly_data(fsym: str, tsym: str, start_date: str, end_date: str = None, limit=2000) -> pd.DataFrame:
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')

    all_data = []
    toTs = datetime.timestamp(datetime.strptime(end_date, '%Y-%m-%d'))
    start_timestamp = datetime.timestamp(datetime.strptime(start_date, '%Y-%m-%d'))
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

            data_chunk = pd.DataFrame(batch)
            data_chunk['time'] = pd.to_datetime(data_chunk['time'], unit='s')  # Vectorized operation for converting time

            # Data Integrity Checks for each chunk
            # ... [perform necessary checks here] ...

            all_data.append(data_chunk)

            if data_chunk['time'].min() < datetime.fromtimestamp(start_timestamp):
                break

            toTs = int(data_chunk['time'].min().timestamp())
        except requests.exceptions.RequestException as e:
            retries += 1
            logging.error(f"Request exception for {fsym}: {e}")
            if retries > MAX_RETRIES:
                logging.error(f"Max retries reached for {fsym}. Last attempt failed with: {e}")
                raise
            logging.info(f"Retrying ({retries}/{MAX_RETRIES}) for {fsym} after failure.")
            time.sleep(10)

    # Concatenate all chunks
    data_df = pd.concat(all_data, ignore_index=True)

    return data_df
