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
