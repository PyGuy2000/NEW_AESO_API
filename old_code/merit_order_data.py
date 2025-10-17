###############################
#MERIT ORDER DATA
################################
import sqlite3
import datetime
import requests
import csv
import pandas as pd
import os
from dotenv import load_dotenv
from src.utilities import create_path
from src.utilities import save_dataframe_to_csv
from tqdm import tqdm
import time
import io

load_dotenv()

# Loop parameters
start_year = 2023
number_of_years = 1  # Update this for the number of years you want to retrieve data for

# AESO API Key
aeso_key =  os.getenv("AESO_OLD_PRIMARY_API_KEY") 
base_url = os.getenv("AESO_OLD_BASE_URL")
output_folder = os.getenv("AESO_OLD_OUTPUT_FOLDER_PATH")


#API End Point
#api_url =  'https://api.aeso.ca/report/v1/meritOrder/energy'
report_name = 'report/v1/meritOrder/energy'
api_url =  base_url + report_name



# Output Folder Name
sub_folder_template= 'Merit Order Curves'

# Output file name
year = None
filename_template = f"merit_order_data_{year}.csv"


# Function to fetch data using the API
def fetch_data_for_date(date, API_KEY,api_url):
    date_str = date.strftime('%Y-%m-%d')
    #print(f" date_str: {date_str}")
    headers = {'accept': 'application/json', 'X-API-Key': API_KEY}
    #api_url = 'https://api.aeso.ca/report/v1/meritOrder/energy'
    params = {'startDate': date_str}
    
    response = requests.get(api_url, headers=headers, params=params)
    
    if response.status_code == 200:
        try:
            #df = pd.read_json(response.text)
            df = pd.read_json(io.StringIO(response.text)) #updated to avoide deprecation warning
            df_json = df.at['data', 'return']
            df_normalized = pd.json_normalize(df_json, 'energy_blocks', ['begin_dateTime_utc', 'begin_dateTime_mpt'])
            # Adjusting column order and adding hour
            desired_order = [
                'begin_dateTime_utc', 'begin_dateTime_mpt', 'import_or_export', 'asset_ID', 'block_number',
                'block_price', 'from_MW', 'to_MW', 'block_size', 'available_MW', 'dispatched?', 
                'dispatched_MW', 'flexible?', 'offer_control'
            ]
            df_normalized = df_normalized[desired_order]
            df_normalized['hour'] = pd.to_datetime(df_normalized['begin_dateTime_utc']).dt.hour
            return df_normalized

        except ValueError as e:
            print(f"Error encountered while loading data into DataFrame: {e}")
            return None
    else:
        print(f"API request failed. Reason: {response.text}")
        return None

# Ensure the Merit Order Curves directory exists
# merit_order_dir = "/kaggle/working/Merit Order Curves"
# if not os.path.exists(merit_order_dir):
#     os.makedirs(merit_order_dir)

# Loop through each year
for year in tqdm(range(start_year, start_year + number_of_years)):
    start_date = datetime.date(year, 1, 1)
    end_date = datetime.date(year, 12, 31)
    current_date = start_date

    # Calculate the total hours in the year for the inner progress bar
    total_hours = int((end_date - start_date).total_seconds() / 3600) + 1

    # Inner loop for hours within each year
    with tqdm(total=total_hours, desc='Hour', position=1, leave=False) as pbar:
        while current_date <= end_date:
            fetched_data = fetch_data_for_date(current_date, aeso_key,api_url)

            # Update the inner progress bar
            pbar.update(1)

            if fetched_data is not None:
                path = create_path(output_folder, sub_folder_template, filename_template, year=year) 
                # Save the DataFrame using the function 
                save_dataframe_to_csv(fetched_data, path) 

            current_date += datetime.timedelta(days=1)  # Move to the next day
            # Increment the current date by 1 hour
            current_date += datetime.timedelta(hours=1)

