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
    sol_data = sol_data[(sol_data['open'] != 0) & (sol_data['high'] != 0) & (sol_data['low'] != 0) & (sol_data['close'] != 0)]

    # Rewrite the cleaned data to csv
    sol_data.to_csv(csv_path, index=False)

    print(f"Cleaned data has been written to {csv_path}, original data is backed up at {backup_path}.")
