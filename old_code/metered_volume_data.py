################################
#METERED VOLUME DATA API
################################

import pandas as pd
from pandas import json_normalize 
import requests
import sqlite3
import datetime
import csv
import pandas as pd
import tqdm
import os
import json
from dotenv import load_dotenv
from functions import create_path
from functions import save_dataframe_to_csv

load_dotenv()


# Specify your start and end dates
start_date = datetime.date(2024, 1, 1)
end_date = datetime.date(2024, 6, 24)
current_date = start_date
#end_date_year = Year(end_date)
end_date_year = end_date.year  # This extracts the year part from the end_date

# AESO API Key
aeso_key =  os.getenv("API_KEY") 
base_url = os.getenv("BASE_URL")
output_folder = os.getenv("OUTPUT_FOLDER_PATH")

#API End Point
#api_url = 'https://api.aeso.ca/report/v1/meteredvolume/details'
report_name = 'report/v1/meteredvolume/details'
api_url =  base_url + report_name


# Output Folder Name
sub_folder_template = ""

# Output file name
year = None
filename_template = f"merit_order_data_{year}.csv"
json_file_name = 'response_data.jsonn'

# This routine also has to read files for processing and combines various 
# files to provide the option to see separate and consolidated files.  The
# code to do this is more complex than the other api calls


def fetch_metered_volume_data(date,aeso_key,api_url):

    # Convert the date object to a string in the format YYYY-MM-DD
    date_str = date.strftime('%Y-%m-%d')
    #print (date_str)
    
    #def fetch_metered_volume_Data(api_url, headers, date, params): 
    #API_KEY = api_key
    headers = {'accept': 'application/json', 'X-API-Key': aeso_key}
 
    #new url
    #api_url = 'https://api.aeso.ca/report/v1/meteredvolume/details'
    
    params = {'startDate': date_str}
    
    #print(f" Date: {date}")

    # Make the API request
    print("Making API Request")
    response = requests.get(api_url, headers=headers, params=params)
    print(response)
    
    # Print the raw data
    #print("Raw Response:")
    #print(response.text)
    
    data = response.text
    #print(data)
    datajson = response.json()

    path = create_path(output_folder, sub_folder_template, json_file_name, year=year) 

    with open(path, 'w') as json_file:
        json.dump(datajson, json_file)

    print("Reading json file")
    df = pd.read_json(data)
    print("Normalizing json file")
    df = pd.json_normalize(df['return'], record_path=['asset_list']).explode('metered_volume_list')
    print("Concatenating data")
    df = pd.concat([df[['asset_ID', 'asset_class']].reset_index(drop=True), pd.json_normalize(df.metered_volume_list)], axis=1)

    #print(df)
    #print(df.to_markdown())

    
    return df #print(df.to_markdown()) #df_normalized


def process_and_save_data(df):
    # Step 1: Create a unique list of asset_ID
    unique_asset_ids = df['asset_ID'].unique()
    print("Unique Asset IDs:")
    print(unique_asset_ids)

    # Step 2: Create a list of unique asset_class entries
    unique_asset_classes = df['asset_class'].unique()
    print("\nUnique Asset Classes:")
    print(unique_asset_classes)

    # Step 3: Create separate CSV files for each asset_class
    for asset_class in unique_asset_classes:
        subset_df = df[df['asset_class'] == asset_class]
        
        #new
        # Reshape the data for this asset_class
        reshaped_data = reshape_data(subset_df)
        
        filename = f"{output_folder}{asset_class}.csv"
        reshaped_data.to_csv(filename, index=False)
        print(f"\nSaved reshaped data for {asset_class} to {filename}")

        
# def save_data_by_asset_class(df):
#     # Create a new column for combined asset_ID and asset_class
#     df['id_class'] = df['asset_ID'] + ' (' + df['asset_class'] + ')'
#     print(df)
#     # Pivot the table
#     pivoted_df = df.pivot(index='begin_date_utc', columns='id_class', values='metered_volume')
#     print(pivoted_df)
#     # Convert the columns to a multi-index
#     pivoted_df.columns = pd.MultiIndex.from_tuples(
#         [tuple(c.split(' (')) for c in pivoted_df.columns],
#         names=['Asset_ID', 'Asset_Class']
#     )

#     # Save separate CSVs for each unique asset_class
#     for asset_class in df['asset_class'].unique():
#         subset_df = pivoted_df.xs(asset_class, axis=1, level='Asset_Class')
#         filename = f"{output_folder}{asset_class}.csv"
#         subset_df.to_csv(filename)
#         print(f"Saved data for {asset_class} to {filename}")
        

def reshape_data(df):
    # Use pivot_table to get the desired format
    pivoted_df = df.pivot_table(index=['begin_date_utc', 'begin_date_mpt'], 
                                columns='asset_ID', 
                                values='metered_volume', 
                                aggfunc='first').reset_index()
    
    return pivoted_df
###########################################################
#NEW
def consolidate_files(output_folder):
    # Step 1: Read the IPP.csv file
    ipp_df = pd.read_csv(f"{output_folder}IPP.csv")

    # Step 2: Remove columns that match the pattern G###
    columns_to_drop = [col for col in ipp_df.columns if col.startswith('G') and len(col) == 4 and col[1:].isdigit()]
    ipp_df.drop(columns=columns_to_drop, inplace=True)

    # Step 3: Read the GENCO.csv file and append it to the ipp_df, excluding the first two columns
    genco_df = pd.read_csv(f"{output_folder}GENCO.csv")
    genco_df.drop(columns=['begin_date_utc', 'begin_date_mpt'], inplace=True)
    
    # Ensure both dataframes are aligned on their index before concatenating
    combined_df = pd.concat([ipp_df, genco_df], axis=1)

    # Step 4: Save the combined DataFrame to "Consolidated Generation Metered Volumes.csv"
    combined_filename = f"{output_folder}Consolidated Generation Metered Volumes.csv"
    combined_df.to_csv(combined_filename, index=False)

    return combined_filename
#############################################################
##########################################


Counter = 0
print(f" Counter: {Counter}")

all_data = []

master_asset_ids = set()
new_asset_ids_per_day = {}

while current_date <= end_date:
    try:
        print("Fetching Data")
        fetched_data = fetch_metered_volume_data(current_date,aeso_key,api_url)
        print(f" fetched_data: {fetched_data}")
        
        print("Extracting unique Asset_IDs")
        # Extract unique Asset_IDs for the day
        day_asset_ids = set(fetched_data['asset_ID'].unique())
        
        print("Find new Asset_IDs for the day")
        # Find new Asset_IDs for the day
        new_ids = day_asset_ids - master_asset_ids
        
        print("update the master list")
        # Update the master list
        master_asset_ids.update(new_ids)
        
        print("Store the new IDs for the day")
        # Store the new IDs for the day
        if new_ids:
            new_asset_ids_per_day[current_date] = new_ids
        
        print("Appending Data")
        all_data.append(fetched_data)
        current_date += datetime.timedelta(days=1)  # Move to the next day
        print(Counter)
        Counter = Counter + 1 
    except Exception as e:
        print(f"Error on date {current_date}: {e}")

        
# Combine all fetched data into one dataframe
print("Combining all fetched data into one dateframe")
df_combined = pd.concat(all_data, ignore_index=True)
print("Process and save the data as 1 file broken down by Asset_Class")
# Process and save the data as 1 file broken down by Asset_Class
process_and_save_data(df_combined)
print("Process and save the data in separate files by Asset_Class")
# Process and save the data in separate files by Asset_Class
#save_data_to_kaggle_output(df_combined)

#########################################################################
#New
# Add the function call at the end of your routine
consolidated_file_path = consolidate_files(output_folder)

import shutil
shutil.make_archive(f' Metered_Volumes{end_date_year}', 'zip', 'C:/Users/kaczanor/OneDrive - Enbridge Inc/Documents/Python/Repaired AESO API Model/output')

