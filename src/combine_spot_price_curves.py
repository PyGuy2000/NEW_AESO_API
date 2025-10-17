import pandas as pd
import glob
import os
import re
from src.utilities import create_path, save_dataframe_to_csv
from src.api_tools import get_api_credientials


'''
Explanation:
Import Libraries: Import pandas for data manipulation and glob and os for file handling.
Define Directory: Set the directory containing the CSV files.
Glob for Files: Use glob.glob to find all CSV files matching the pattern pool_price_data_*.csv within the directory.
Read and Process Each File:
Loop through each file path returned by glob.
Read the CSV file into a pandas DataFrame.
Convert the 'begin_datetime_mpt' column to a datetime object.
Append the DataFrame to a list.
Concatenate DataFrames: Use pd.concat to merge all DataFrames in the list into a single DataFrame.
Set Index: Optionally, set 'begin_datetime_mpt' as the index of the merged DataFrame.
Save Merged Data: Save the merged DataFrame to a new CSV file named `merged_pool
'''

# Define the directory containing the CSV files
directory = r'C:\Users\kaczanor\OneDrive - Enbridge Inc\Documents\Python\Revised-AESO-API-master\output\Spot_Prices'

#Define the api serivices you are running
services = ['AESO_OLD']
for service in services:
    output_folder = get_api_credientials(service)

# Create an empty list to store individual DataFrames
data_frames = []

# Use glob to match the pattern of the files
csv_files = glob.glob(os.path.join(directory, "pool_price_data_*.csv"))

# Extract years from filenames
years = []

# Loop through the matched CSV files
for file in csv_files:
    
    # Extract year from filename using regex
    match = re.search(r'pool_price_data_(\d{4})', file)
    if match:
        year = int(match.group(1))
        years.append(year)
    
    # Read each CSV file into a DataFrame
    df = pd.read_csv(file)
    
    # Convert 'begin_datetime_mpt' to datetime
    df['begin_datetime_mpt'] = pd.to_datetime(df['begin_datetime_mpt'])
    
    # Append the DataFrame to the list
    data_frames.append(df)

# Concatenate all DataFrames in the list
merged_df = pd.concat(data_frames, ignore_index=True)

# Optionally, set 'begin_datetime_mpt' as the index
merged_df.set_index('begin_datetime_mpt', inplace=True)

# Get the start and end years
start_year = min(years)
end_year = max(years)

# Save the merged DataFrame to a new CSV file with the start and end years in the filename
#output_file = os.path.join(directory, f'merged_pool_price_data_{start_year}_to_{end_year}.csv')
#merged_df.to_csv(output_file)

#print(f"Merged data saved to {output_file}")

# Save the combined data frame
filename = f'merged_pool_price_data_{start_year}_to_{end_year}.csv'
subfolder = "Spot Prices"
path = create_path(output_folder, subfolder, filename)
save_dataframe_to_csv(merged_df, path) 


# ###########################################
