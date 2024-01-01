import pandas as pd
import os
from shutil import copyfile


def clean_sol_data(csv_path='sol_data.csv'):
    """
    Cleans the Solana data CSV file by removing rows where 'high', 'low', 'open', and 'close' are all zero.
    A backup of the original file is created before performing the operation.

    Parameters:
    csv_path (str): Path to the Solana data CSV file. Default is 'sol_data.csv'.
    """

    # Check if the file exists
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"The file {csv_path} does not exist.")

    # Create a backup of the original file
    backup_path = csv_path.replace('.csv', '_backup.csv')
    copyfile(csv_path, backup_path)

    # Read the CSV file
    sol_data = pd.read_csv(csv_path)

    # Validate required columns
    required_columns = ['high', 'low', 'open', 'close']
    if not all(column in sol_data.columns for column in required_columns):
        raise ValueError("CSV file is missing one or more required columns.")

    # Drop rows where 'high', 'low', 'open', and 'close' are all zero
    sol_data = sol_data[
        (sol_data['open'] != 0) & (sol_data['high'] != 0) & (sol_data['low'] != 0) & (sol_data['close'] != 0)]

    # Rewrite the cleaned data to csv
    sol_data.to_csv(csv_path, index=False)

    print(f"Cleaned data has been written to {csv_path}, original data is backed up at {backup_path}.")


def transform_crypto_data(csv_path, crypto_prefix):
    """
    Transforms cryptocurrency data in a CSV file.

    Parameters:
    csv_path (str): Path to the cryptocurrency data CSV file.
    crypto_prefix (str): Prefix for the cryptocurrency (e.g., 'BTC', 'ETH', 'SOL').

    The function performs the following transformations:
    - Generates a record_id column with the crypto prefix followed by a sequential integer.
    - Converts the time column from Unix timestamp to YYYY-MM-DD format and renames it to 'date'.
    - Renames 'volumefrom' to 'trade_vol_native'.
    - Renames 'volumeto' to 'trade_vol_USD' and refactors it to show the full numeric value.
    - Drops the 'conversionType' and 'conversionSymbol' columns.
    """

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"The file {csv_path} does not exist.")

    # Read the CSV file
    data = pd.read_csv(csv_path)

    # Generate record_id
    data['record_id'] = [f"{crypto_prefix}{str(i).zfill(2)}" for i in range(1, len(data) + 1)]

    # Convert 'time' column to 'date'
    data['date'] = pd.to_datetime(data['time'], unit='s').dt.strftime('%Y-%m-%d')
    data.drop('time', axis=1, inplace=True)

    # Rename columns
    data.rename(columns={'volumefrom': 'trade_vol_native', 'volumeto': 'trade_vol_USD'}, inplace=True)

    # Refactor 'trade_vol_USD' to full numeric value
    data['trade_vol_USD'] = data['trade_vol_USD'].apply(lambda x: '{:.2f}'.format(x))

    # Drop unnecessary columns
    data.drop(['conversionType', 'conversionSymbol'], axis=1, inplace=True)

    # Save the transformed data
    data.to_csv(csv_path, index=False)

    return data

# Example usage
# transform_crypto_data('sol_data.csv', 'SOL')
