#Main Module


import os
import sys
import pandas as pd
import datetime
from dotenv import load_dotenv
import src.utilities as utilities
from src.utilities import (
    print_directory_tree,
    print_folder_list,
    remove_folder_contents,
    create_path,
    fetch_data,
    create_sqlite_table,
    save_to_sqlite,
    execute_sql_query,
    consolidate_annual_files
)

from src.api_tools import (
    build_api_request_repository,
    get_api_credientials
)

import requests
from tqdm import tqdm
import io

##############################################################################
#Set-up
##############################################################################


print_directory_tree('C:/Users/kaczanor/OneDrive - Enbridge Inc/Documents/Python/Revised-AESO-API-master')

#Loads the .env file
load_dotenv()

#Define the api serivices you are running
services = ['AESO_NEW'] # ['AESO_OLD' and 'AESO_NEW']


csv_output = True
sqlite_output = False
remove_existing_output_files = False


##############################################################################
#Active API Calls
##############################################################################
# Special Notes
# 1) Keep Asset List up to date as it feeds other queries
# 2) the Historical Hourly Demand Data has multiple steps as it needs to be injested into the AB_Historical
#    model with a deamnd data set merged with export data. The export data is extracted from the Metered Volume
#    api call and further refined using the Asset_List api call to refine the imports/exports
#    a) Run the AIL Demand file whicvh creates "Metered_Demand_YYYY.csv" in the folder "Historical AIL Demand"
#       [begin_datetime_utc, begin_datetime_mpt, alberta_internal_load, forecast_alberta_internal_load]

#    b) Run Asset_List api 

#    c) Run Metered Volumne run for same dates and this creates data in the "Metered Volume" folder

#    d) temp files are also created that breakdown the import/exports by entity.  Those file are are called:
#       ("export_categorized_filtered_sorted.csv" and "import_categorized_filtered_sorted.csv") with the following columns:
#       [begin_date_utc,begin_date_mpt,ASSET_ID,TOTAL_EXPORTS,ASSET_NAME,ASSET_TYPE,OPERATING_STATUS,
#                   POOL_PARTICIPANT_NAME,POOL_PARTICIPANT_ID,NET_TO_GRID_ASSET_FLAG,ASSET_INCL_STORAGE_FLAG,REGION]

#    e) Those detailed import/export files are then summarized into a file called "aggregated_hourly_import_export_data.csv" 
#       which has the following column headers:  [begin_date_utc, begin_date_mpt, TOTAL_IMPORTS, TOTAL_EXPORTS

#    f) Finally the "Metered_Demand_YYYY.csv"v are merged with the

api_activation_dict = {
    'pool_participant_data_state' :  False,                         # Pulls Alberta Pool Participant Demand Data
    'operating_reserve_offer_control_data_state' : False,           # Pulls Alberta Operating Reserve Offer Control Data
    'actual_forecast_report_data_state' : True,                    # Pulls Alberta Historical Hourly Demand Data
    'asset_list_data_state' : True,                                # Pulls Alberta Asset List
    'generators_above_5MW_data_state' : False,                      # Pulls Alberta Power Generator List
    'historical_spot_price_specific_date_and_range_state' : True,  # Pulls Alberta Historical Hourly Pool Price
    'historical_spot_price_specific_date_state' : False,            # Pulls Alberta Historical Hourly Pool Price >>> appearas to only pull 6 months of data >>>Check this
    'merit_order_data_state' : False,                               # Pulls Alberta Hourly Merit Order Data
    'metered_volume_data_state' : False,                             # Pulls Alberta Hourly Power Generation Production Data
    'supply_demand_data_generation_state' : False,                  # Pulls Alberta Current Snapshot of AESO Supply Demand Page
    'supply_demand_data_intertie_state' : False,                    # Pulls Alberta Current Snapshot of AESO Supply Demand Page
    'supply_demand_data_summary_state' : False,                     # Pulls Alberta Current Snapshot of AESO Supply Demand Page
    'system_marginal_price_data_state' : False                      # Pulls Hourly Spot Price
}


##############################################################################
#Time-based Inputs
##############################################################################
print_folder_list()
start_date = datetime.date(2025, 1, 1)
explicit_end_date = datetime.date(2025, 10, 16)
end_date = f"{start_date.year}-12-31"
# new
end_date = datetime.date(start_date.year, 12, 31)
current_date = start_date
start_date_year = start_date.year
end_date_year = explicit_end_date.year
number_of_years = end_date_year - start_date_year + 1
print(f"number of year:{number_of_years}")
explicit_end_date_year = explicit_end_date.year  # Extract the year part from the explicit_end_date
str_start_date = start_date.strftime("%Y-%m-%d")
str_explicit_end_date = explicit_end_date.strftime("%Y-%m-%d")

#date_str = None 
date_str = current_date.strftime('%Y-%m-%d')
year = None
operating_status = 'ALL' #None
asset_type = 'ALL' #None

##############################################################################
#CREATE API CALL DICTIONARY
##############################################################################

#Loop through the api services chosen and load credentials and api parameters/headers/keys
for service in services:
    aeso_key, base_url, output_folder = get_api_credientials(service)
    api_function_call_dict = build_api_request_repository(
                    api_activation_dict,
                    aeso_key,
                    base_url, 
                    start_date, 
                    end_date, 
                    explicit_end_date, 
                    str_start_date, 
                    str_explicit_end_date, 
                    year, 
                    operating_status, 
                    asset_type, 
                    output_folder)
    
    print(f"API Function Call Dictionary: {api_function_call_dict}")

#Remove or retain existing output files
if remove_existing_output_files:
    #output_folder = api_function_call_dict[]
    #Remove existing contente from data folders
    remove_folder_contents(output_folder)
    print(f" base_output_directory_global: {output_folder}")


#############################################################################
#Set-up SQLite Data Base
#############################################################################
if sqlite_output == True:
    file_name_template = None
    #create_sqlite_database
    sub_folder_template = 'SQLite/'
    db_file_name = 'Alberta_Houlry_Merit_Order_Test_File.db'
    db_table_name = 'merit_order_daily_hourly_data'
    data_base_full_path = create_path(output_folder, sub_folder_template, file_name_template)
    conn = create_sqlite_table(data_base_full_path,db_table_name)
else:
    db_file_name = None
    db_table_name = None
    conn = None


#-----------------------------------------------------------------------------
# Loop through the dictionary and make the API calls
try:
    category_key = None
    ##############################
    # Step1 :  # Loop through API Call Dictionary to decide what to run
    ##############################
    for entity_key, entity_value in api_function_call_dict.items():
        print(f" {entity_key} and {entity_value}")
        for category_key, api_config in entity_value.items():
            if 'output_consolidated_csv_files' not in api_config:
                print(f"Missing 'output_consolidated_csv_files' in entity: {entity_key}, category: {category_key}")
            else:
                pass
            output_csv_files = api_config['output_csv_files']
            function_name =  api_config['function_name']
            data_type = api_config['data_type']
            sub_folder_template = api_config['sub_folder_template']
            file_name_template = api_config['file_name_template']
            column_order = api_config['column_order']
            run_option = api_config['run_option']
            consolidate_files = api_config['consolidate_files']
            output_consolidated_csv_files = api_config['output_consolidated_csv_files']

            ###############################
            # Step 2 :  Only run API Calls with run_option == True
            ###############################
            if run_option:
                print(f"api_config['run_option']: {function_name}")
                print(f"Fetching data for {category_key}...")
                #print(f" First file_name_template :{file_name_template}")
                
                
                try:
                    ###############################
                    # Step 3: Loop through annual data to create annual output files
                    # Note some API Calls result in lists and only have 1x output file. Other API Calls are based on start and end dates and
                    # produce multiple annual files and the year loop below facilitaties this.  Lastly, some API calls only taka a start date. 
                    # For example the metered volume API call only takes a start date and the end date is not required.  These result in API responses
                    # that are daily.  In order to produce multiple daily perods a While Loop is required to make this API call for each day in the range.
                    # Thus when the metered volume API call is made the annual loop below cannot be used to increment the files by year.  Even though
                    # the code stil runs that annual loop, the loop that actually handles the daily API calls in another sub-routine in Step 7 which calls its own
                    # fetch_data function. The fetch_data function is the same as the one in Step 4 but it is called in a different function. This is because the
                    # metered volume API call is the only one that requires a daily loop.  The other API calls are either list based or annual based.
                    ###############################
                    year_counter = 0
                    for year in tqdm(range(start_date_year, start_date_year + number_of_years)):
                        year_counter = year_counter + 1
                        print(f"number_of_years: {number_of_years}")
                        print(f"year range: {range(start_date_year, start_date_year + number_of_years)}")

                        # Update Dates and Convert to strings in order to pass the dictionary
                        # This creates a new start and end date for each year in the loop so that each year can be processed
                        # and saved as a separate annual file
                        updated_start_date = f"{year}-01-01"
                        
                        if year < end_date_year:
                            updated_end_date = f"{year}-12-31"
                        else:
                            updated_end_date = explicit_end_date.strftime('%Y-%m-%d')
                            
                        print(f" start_date and end_date within annual loop: {updated_start_date, updated_end_date}")
                    
                        # Need to also pass date data to the call for files that produce more than 1 file
                        print(f"Making API call for {category_key}")

                        ###################################
                        #Step 4: Make API Call
                        fetched_data_df = fetch_data(api_config, updated_start_date, updated_end_date)
                        print(f" fetched_data_df: {fetched_data_df}")
                        ###################################
                        
                        # Convert dates back to date format
                        updated_start_date = pd.to_datetime(updated_start_date)
                        updated_end_date = pd.to_datetime(updated_end_date)
                        explicit_end_date = pd.to_datetime(explicit_end_date)

                        ###################################
                        # Step 5: Review the fetched data from the returned API call
                        ###################################
                        if fetched_data_df is not None:
                        
                            #Post-process data
                            print(f"Calling Post Processing Function for {category_key} for {year}")

                            print(f" print function name: {function_name}")

                            ###################################
                            # Step 6: Retrieve name of post processing function from API dictionary
                            ###################################
                            post_process_function = getattr(utilities, function_name) 
                            

                            print(f" post_process_function: {post_process_function}")
                            print(f" output_folder: {output_folder}")
                            print(f" sub_folder_template: {sub_folder_template}")
                            print(f" file_name_template: {file_name_template}")
                            
                            # Ouput files in csv format with either be single files for lists or they will be 
                            # annual files for time series data.  The file path has the filename which will be updated


                            # Output files in SQLite db format will only ever be one file so there is no need to create
                            # a dynamic file name that can take mulitiple {year} extensions in the file name
                            year_str = str(year)
                            print(f" year_str: {year_str}")
                            print(f" Second file_name_template: {file_name_template}")
                            updated_file_name_template = file_name_template.replace('None', year_str)
                            print(f" Third updated_file_name_template: {updated_file_name_template}")
                            path = create_path(output_folder, sub_folder_template, updated_file_name_template)
                            
                            print(f" path: {path}")
                            print(f" fetched_data_df: {fetched_data_df}")
                            
                            ###################################
                            # Step 7: Call customer processing function specific to the API call being made
                            # Each api call has a different post processing routine. The API Call diction has an item called
                            # "function_name" which is a text string representation of the actual functon that needs to be called.
                            # That text string is passed to a function called "post_process_function" along with all the required parameters. 
                            # The "post_process_function" redirects the function call to a function with that exact name.  This allows the code
                            # to only have mone master function call instead of multiple calls.
                             ###################################
                             
                            processed_data_df = post_process_function(api_config, fetched_data_df, output_csv_files, updated_start_date, updated_end_date, explicit_end_date, year, \
                                path, csv_output, sqlite_output, conn, db_table_name, column_order)

                            
                        else:
                            print(f"Failed to fetch data for {category_key}")

                    #Consolidate annual files if option set to True for API Call
                    if fetched_data_df is not None:
                        if consolidate_files:
                                    consolidated_df = consolidate_annual_files(api_config, output_folder, output_consolidated_csv_files, sub_folder_template, csv_output, \
                                        sqlite_output, conn, db_table_name, column_order)

                except Exception as e:
                    print(f"An error occurred with DataFrame: {category_key}")
                    print(f"Error: {e}")        
            else:
                print(f"Did not run {category_key}")

except Exception as e:
            print(f"An error occurred with DataFrame: {category_key}")
            print(f"Error: {e}")