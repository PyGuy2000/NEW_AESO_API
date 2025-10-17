#import main
import pandas as pd

# Request library
# old Used with old wagger API Gateway
import requests
#new used with new 'Azure APIM API Gateway'
import urllib.request
# Speical Note:
# When transitioning from using the requests library to urllib.request, there are a 
# few differences to be aware of. The requests library is designed to be more user-friendly, 
# especially for handling HTTP requests and responses. On the other hand, 
# urllib.request is a lower-level library, which may require more manual handling of 
# URL parameters and headers.  The urllib.request.Request does not directly support a 
# params argument like requests.get does. Instead, you need to manually construct the URL with
# parameters.Manually. To adapt the code you:
# 1) build the URL with Parameters: Use Python's urllib.parse to construct the URL with query parameters.
# 2) Pass Headers Correctly: Ensure headers are passed as a dictionary to urllib.request.Request.


import os
from dotenv import load_dotenv
import datetime
import csv
import sqlite3
from tqdm import tqdm
import json
import glob
import re

from src.aggregate_imports_and_exports import aggregate_import_exports
from src.combine_ail_demand_exports_imports import combine_demand_with_tie_line_data, append_aggregated_annual_data_with_tie_line_data

load_dotenv()
###############################################
#SQLite Functions
###############################################

#----------------------------------------------
#create SQLite Table
def create_sqlite_table(db_name,db_table_name):

    conn = sqlite3.connect(db_name)
    
    conn.execute('VACUUM;')
    # Set journal mode to WAL for better write performance
    conn.execute("PRAGMA journal_mode=WAL;")
    # Set synchronous to NORMAL for a balance between speed and data safety
    conn.execute("PRAGMA synchronous=NORMAL;")
    
    cursor = conn.cursor()

    print("SQL Connection Established")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS MeritOrder (
    Timestamp_UTC TEXT, 
    Timestamp_Local TEXT, 
    EnergyBlocks TEXT,
    AssetID TEXT, 
    BlockNumber INTEGER, 
    BlockBid REAL, 
    BlocksFromMW INTEGER, 
    BlocksToMW INTEGER, 
    BlockSize INTEGER, 
    BlocksAvailable INTEGER, 
    BlocksDispatchedMW INTEGER, 
    BlocksFlexible TEXT, 
    BlockOfferControl TEXT
)
""")

    cursor.execute(create_sqlite_table)

    # Commit the transaction.
    conn.commit()
    
    print("SQL Table Created")

    return conn

#----------------------------------------------
# Function to save the fetched data to SQLite
def save_to_sqlite(data, table_name, columns, connection):
    # Convert DataFrame to list of lists
    rows = data.values.tolist()
    
    cursor = connection.cursor()
    
    for row in tqdm.tqdm(data.iloc[1:, :].values):
        while len(row) < len(columns):
            row.append(None)
            
        query = f"""
            INSERT INTO {table_name} ({', '.join(columns)}) 
            VALUES ({', '.join(['?' for _ in columns])})
        """
        cursor.execute(query, tuple(row[:len(columns)]))
    
    # Confirm tables are created
    print("check")
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    print(cursor.fetchall())
    
    connection.commit()
#----------------------------------------------
def execute_sql_query(cursor, table, columns, data):
    for row in tqdm.tqdm(data.iloc[1:, :].values):
        while len(row) < len(columns):
            row.append(None)
        query = f"""
        INSERT INTO {table} ({', '.join(columns)}) 
        VALUES ({', '.join(['?' for _ in columns])})
        """
        cursor.execute(query, tuple(row[:len(columns)]))

###############################################
# Function to Remove Folder Content at Start-up, Create File Paths and Save Data During New Run
###############################################
def print_folder_list():
    # get the path of the current script
    script_path = os.path.abspath(__file__)

    # get the directory that contains the script (root folder of the project)
    root_folder = os.path.dirname(script_path)

    # loop through all files in the root folder
    for filename in os.listdir(root_folder):
        # check if the file has a .py extension
        if filename.endswith('.py'):
            # print the filename
            print(filename)

    return



# Function to remove folder contents at start-up and create csv and image folders to save data in.
def remove_folder_contents(folder):
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                remove_folder_contents(file_path)
                os.rmdir(file_path)
        except Exception as e:
            print(e)


def create_path(output_folder, sub_folder_template, filename_template, **kwargs): 
    """ Create a file path using templates for subfolder and filename with given placeholders. 
        Parameters: output_folder (str): The base output folder path. sub_folder_template (str): 
        The template for the subfolder with placeholders. filename_template (str): 
        The template for the filename with placeholders. kwargs:
        Additional keyword arguments to replace in the templates. 
        Returns: str: The complete file path. 
        """ 
    sub_folder = sub_folder_template.format(**kwargs) 
    filename = filename_template.format(**kwargs) 
    path = os.path.join(output_folder, sub_folder, filename) 
    
    return path 
        
def save_dataframe_to_csv(df, path): 
    """ Save the DataFrame to a CSV file at the specified path. Parameters: df (pd.DataFrame): 
        The DataFrame to save. path (str): The complete path where the file should be saved. 
        """ 
    #print(f" calculation of path in save_data_frame_to_csv function: {path}")
    
    # Ensure the directory exists (if not, create it) 
    if not os.path.exists(os.path.dirname(path)): 
        os.makedirs(os.path.dirname(path)) 
    
    # Export the DataFrame to the specified CSV file 
    df.to_csv(path, index=False) 

###############################################
# Function to Create Directory Tree
###############################################

def print_directory_tree(root_dir, padding=''):
    # Print the root directory
    print(padding[:-1] + '+--' + os.path.basename(root_dir) + '/')

    # Get the list of files and directories in the root directory
    contents = os.listdir(root_dir)

    # Recursively print the directory tree structure for each subdirectory
    for item in contents:
        item_path = os.path.join(root_dir, item)
        if os.path.isdir(item_path):
            print_directory_tree(item_path, padding + '    ')
        else:
            print(padding + '|--' + item)

    #print_directory_tree('C:/Users/Rob_Kaz/Documents/Rob Personal Documents/Python/Revised-AESO-API-master/')

#####################################
# Function to do preliminary processing of the data
#####################################
def preliminary_processing_data(api_config, response_json):
    print("Processing data...")
    
    # reporting_limit = api_config['reporting_limit']
    # headers = api_config['headers']
    # params = api_config['params']
    # api_url = api_config['api_url']
    return_key = api_config.get('return_key')
    normalize_json = api_config.get('normalize_json')
    record_path_for_normalize_json = api_config.get('record_path_for_normalize_json')
    json_normalize_keys= api_config.get('json_normalize_keys')
    meta_for_normalized_json = api_config.get('meta_for_normalized_json') 
    normalized_concatenation_keys = api_config['normalized_concatenation_keys']
    json_explode = api_config.get('json_explode')
    removed_data_lists = api_config.get('removed_data_lists') 
    column_order = api_config.get('column_order', None)

    # json responses can either come back as a list or a dictionary. For example, the pool price reports
    # come back as dictionaries that reference a "Report" (See Example 1 and 2 below). This requires us to
    # inlcude the 'key' (the report) of the dictionary in the functions return statement.  This is becuase we have
    # to use the key when creating the processed data frame. Whereas other api calls return a list like in Example 3.
    # This only requires us to pass the list itself to the data frame.  To mange this there is copndition logic that 
    # checks the return type and either includes a list or a key in the return statement

    '''
    Example (Dictionary): #1 System Marginal Price
    {
    "timestamp": "2024-06-21 21:28:12.131+0000",
    "responseCode": "200",
    "return": {
        "System Marginal Price Report": [
    {
        "begin_datetime_utc": "2000-01-03 06:21",
        "end_datetime_utc": "2000-01-03 07:00",
        "begin_datetime_mpt": "2000-01-02 23:21",
        "end_datetime_mpt": "2000-01-03 00:00",
        "system_marginal_price": "30.44",
        "volume": ""
    },
    
    Example (Dictionary): #2 Pool Price Report
    {
    "timestamp": "2024-06-21 21:27:16.947+0000",
    "responseCode": "200",
    "return": {
        "Pool Price Report": [
        {
            "begin_datetime_utc": "2000-01-01 07:00",
            "begin_datetime_mpt": "2000-01-01 00:00",
            "pool_price": "21.65",
            "forecast_pool_price": "30.48",
            "rolling_30day_avg": "40.12"
        },
    
    Example #3 (List): Metered Volumnes
    {
    "timestamp": "2024-06-21 21:29:43.634+0000",
    "responseCode": "200",
    "return": [
        {
        "pool_participant_ID": "ACC",
        "asset_list": [
            {
            "asset_ID": "ACBC",
            "asset_class": "IMPORTER",
            "metered_volume_list": [
                {
                "begin_date_utc": "2000-01-01 07:00",
                "begin_date_mpt": "2000-01-01 00:00",
                "metered_volume": "0"
                },
    '''
        
    # add a check to handle this case. If the "return" value in the JSON response is a list, we can directly pass it to pd.DataFrame(). If not
    # the key of the dictionary is included in the return statment and used when creating the data frame.abs

    # JSON (JavaScript Object Notation) is a popular data format that stores information in key-value pairs or arrays, 
    # and it can be nested (i.e., values can be other JSON objects or arrays). This makes JSON flexible and human-readable, 
    # but it's not always convenient for analysis or machine processing, which often assumes a tabular, or flat, data format.
    #
    # Normalization: Normalizing JSON data involves flattening it into a table structure. This is useful when you have nested 
    # JSON data but you want to analyze it using tools or libraries (like pandas) that work better with flat tables (DataFrames). 
    # For example, if you have a JSON object with a key whose value is an array of other JSON objects, normalizing that data would 
    # create a row for each object in the array, with columns corresponding to the keys in the nested objects.
    #
    # Exploding: Exploding is a specific kind of normalization where each element of a list (or array, in JSON) in a DataFrame 
    # cell is expanded into its own row in the DataFrame. The index values are duplicated for these new rows, so you still 
    # have a link back to the original data. This is useful when you have a list of values in a single DataFrame cell, but you 
    # want to perform operations or analysis on each individual value. For example, if you have a DataFrame of movies and one of 
    # the columns is a list of genres for each movie, you might explode the genres column if you want to analyze each genre separately.

    try:
        
        # Step 3: Review repsonse data that is in json format 
        #df = response.json()
        df = response_json
        # Check the keys within 'return'
        print(f" df.keys: {df.keys()}")
        
        # Ensure 'return_key' exists within the nested 'return' key
        nested_return = df.get('return', {})
        
        if isinstance(df["return"], dict):
            # Debugging print to check the keys inside the nested 'return'
            print(f"Keys in nested 'return': {nested_return.keys()}")
        
        # Print all keys to identify exact key name
        for key in df.keys():
            print(f"Key in 'return': '{key}'")

        # Step 4: If required normalize and or explode json data prior to passing to data frame
        # Loop through json_normalize_keys that are already loaded into the api call dictionary
        # Note those keys MUST be loaded in the correct order.  This code snippet dynamnically identifies
        # the structure of keys  in the api call dictionary item - json_normalize_keys - and loads them
        # a data frame
        if json_normalize_keys:
            for key in json_normalize_keys:
                if key in df:
                    df = df[key]
                    
        # Check if df is a dict with one key, if so, assign the value to df
        if isinstance(df, dict) and len(df) == 1:
            df = list(df.values())[0]
        else:
            pass
        
        if normalize_json:
            # The purpose of this next snippet is to transform a nested JSON structure into a flat table structure that can 
            # be easily manipulated as a pandas dataframe.
            
            print(f"{return_key}, {record_path_for_normalize_json}, {meta_for_normalized_json}")
            
            try:
                print(f"df[return_key]: {nested_return.get(return_key, 'Key not found')}")
            except AttributeError:
                print("'nested_return' is not a dictionary or does not have 'get' method.")
            
            print("API response data needs to normalize the JSON object.")
            
            if json_explode:
                print(f"Normalizing json data......")
                # This line is flattening the nested structure in the column specified by record_path_for_normalize_json.
                # The resulting dataframe df_normalized has a flattened structure for this column.
                print(f" record_path_for_normalize_json: {record_path_for_normalize_json}")
                df_normalized = pd.json_normalize(df, record_path_for_normalize_json)

                print(f"Exploding normalized json data ....")
                # This line is "exploding" lists in the column specified by meta_for_normalized_json into separate rows. 
                # Now, df_normalized has multiple rows for each item in the exploded list.
                print(f" meta_for_normalized_json: {meta_for_normalized_json}")
                df_normalized = df_normalized.explode(meta_for_normalized_json)

                # check if exploded column contains dictionaries and normalize it.abs# If the exploded column contains 
                # dictionaries (meaning it has further nested structure), we need to flatten it. But if we flatten it 
                # directly in the dataframe, we would lose the association between the flattened data and the other 
                # columns in the original row. This is where the concatenation comes in.
                print(f" Prepping exploded/normalized json data for concatnation...")

                if normalized_concatenation_keys:
                    # This is dropping the exploded column that contains dictionaries from df_normalized. We're doing this because we're about to 
                    # replace this column with its flattened version. The concatenation is joining the dataframe that contains the other columns 
                    # (df_normalized[concatenation_keys].reset_index(drop=True)) with the dataframe that contains the flattened version of the 
                    # exploded column. The result is a dataframe that has both the other columns and the flattened version of the exploded column.
                    #if isinstance(df_normalized[normalized_concatenation_keys].iloc[0], dict):
                    print("Concatentating exploded json data.....")
                    print(f" Concatenation Keys {normalized_concatenation_keys}....")
                    #if normalized_concatenation_keys:
                    df_normalized = pd.concat([df_normalized[normalized_concatenation_keys].reset_index(drop=True),
                                    pd.json_normalize(df_normalized[meta_for_normalized_json])], axis=1)
                        
                else:
                    raise ValueError("Concatenation keys must be provided when exploding dictionary data")

            else:
                
                if return_key in nested_return:
                    df_normalized = pd.json_normalize(nested_return[return_key], record_path_for_normalize_json, meta_for_normalized_json)
                else:
                    raise KeyError(f"'{return_key}' not found in the nested 'return' object.")
                
            if column_order:
                print("Reordering column data....")
                df_normalized = df_normalized[column_order]
                print(f" df_normalized.head(): {df_normalized.head()}")
            return df_normalized 
        
        else:
            # Step 5: if data does not require normalization, proceed with 
            # passing data to data frame using the correct return keysS. This first
            # checks to see if the response is a "list" object. If so is simply passes the 
            # the data frame to the return statement. If it is not a list, then it is likely
            # a dictionary and we have to pass both the data frame AND the return key
            print("API response data does not need to normalize the JSON object.")
            
            # Check the type of the response
            print(f"Type of response: {type(df)}")

            #if list, pass df in return statement
            if isinstance(df["return"], list):
                
                print ("API Call returned a list object.")
                return pd.DataFrame(df["return"]) #!!!!!!!!!!
            else:
                print("API Return is not a List")
                #if df returned by api call is not a list it is likel a dictionary then include the key in the return
                if return_key is not None:
                    print(f"return_key = {return_key}")
                    return pd.DataFrame(df["return"][return_key])  #!!!!!!!!!!
                    print ("API Call returned a dictionary object.")
                else:
                    print("API Return is None")
                    
                    ##############################################
                    if removed_data_lists is not None:
                        # initialize counter for removed lists
                        count = 0
                        # iterate over the removed_data_list
                        for item in removed_data_lists:
                            # if the item exists in the dictionary, increment the counter and remove the item
                            print(f" print df : {df}")
                            if item in df['return']:
                                count += 1
                                df['return'].pop(item, None)
                                # print the count
                                print(f"Removed {count} items")
                    return pd.DataFrame(df['return'], index=[0])
                    
                    ################################################
   
            #Lastly, reset column order if required for all other scenarios
            if column_order:
                print("Reordering column data....")
                df_normalized = df_normalized[column_order]
                print(f" df_normalized.head(): {df_normalized.head()}")
                return df_normalized  
    
    except KeyError as e:
        print(f"KeyError encountered while loading data into DataFrame: {e}")
        return None  

#####################################
# Function to handle different API Call status codes
#####################################
def handle_status_code(api_config, response_status_code, response_str):
    # Define actions for each status code
    actions = {
        200: handle_success,
        400: handle_bad_request,
        401: handle_unauthorized,
        403: handle_forbidden,
        404: handle_not_found,
        405: handle_invalid_method,
        500: handle_internal_server_error,
        503: handle_service_unavailable,
    }

    # Get the appropriate handler function based on status code
    #handler = actions.get(response.status_code, handle_generic_error)
    handler = actions.get(response_status_code, handle_generic_error)
    
    # Call the handler function
    return handler(api_config, response_status_code, response_str)
#----------------------------------------------
#def handle_success(api_config, response):
def handle_success(api_config, response_status_code, response_str):
    print("Response code is 200 (OK)")

    # try:
    #     # Attempt to parse response JSON
    #     #old
    #     #data = response.json()
        #   if data:
        #     # Call the function to process data
        #     df = preliminary_processing_data(api_config, response)
        #   else:
        #     print("Response JSON is empty or None")
    #     # When using the requests library, you can easily parse the JSON response using response.json(). 
    #     # However, when using urllib.request, you need to manually handle the conversion of the response
    #     # data from bytes to a JSON object.
    # except json.JSONDecodeError as e:
    #     print(f"Error decoding JSON: {str(e)}")
        
    # except Exception as e:
    #     print(f"An error occurred: {str(e)}")
    # return df
        
        #new
    try:
        # print(f" response content: {response.read().decode('utf-8')}")
        # response_str = response.read().decode('utf-8')

        # Parse the JSON string to a Python object
        if response_str:
            response_json = json.loads(response_str)
            print(f"response_json: {response_json}")
            data_temp = response_json
            # Call the function to process data here if needed
            if data_temp:
                # Call the function to process data
                df = preliminary_processing_data(api_config, response_json)
                return df
        else:
            print("Response JSON is empty or None")

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {str(e)}")
        return None
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

#----------------------------------------------
def handle_bad_request(api_config, response):
    print("Bad Request (400): Check your request parameters")
#----------------------------------------------
def handle_unauthorized(api_config, response):
    print("Unauthorized (401): Authentication failed or missing credentials")
#----------------------------------------------
def handle_forbidden(api_config, response):
    print("Forbidden (403): Access to the resource is denied")
#----------------------------------------------
def handle_not_found(api_config, response):
    print("Not Found (404): The requested resource was not found")
#----------------------------------------------
def handle_invalid_method(api_config, response):
    print("Invalid Method (405): HTTP method not allowed for the requested resource")
#----------------------------------------------
def handle_internal_server_error(api_config, response):
    print("Internal Server Error (500): Something went wrong on the server side")
#----------------------------------------------
def handle_service_unavailable(api_config, response):
    print("Service Unavailable (503): The server is currently unable to handle the request")
#----------------------------------------------
def handle_generic_error(api_config, response):
    print(f"Error: Response code {response.status_code} - {response.reason}")

#####################################
# Function to make the API call
#####################################
def fetch_data(
        api_config, 
        updated_start_date, 
        updated_end_date
        ):
    
    # STEP 1: Take slices of API Call Dictionary to define headers, paramters, and keys for request
    reporting_limit = api_config['reporting_limit']
    headers = api_config['headers']
    print(f"headers:{headers}")
    params = api_config['params']
    api_url = api_config['api_url']
    print(f" api_url: {api_url}")
    return_key = api_config.get('return_key')
    # normalize_json = api_config.get('normalize_json')
    # record_path_for_normalize_json = api_config.get('record_path_for_normalize_json')
    # json_normalize_keys= api_config.get('json_normalize_keys')
    # meta_for_normalized_json = api_config.get('meta_for_normalized_json') 
    # normalized_concatenation_keys = api_config['normalized_concatenation_keys']
    # json_explode = api_config.get('json_explode')
    # removed_data_lists = api_config.get('removed_data_lists') 
    # column_order = api_config.get('column_order', None)
    
    
    # Step #1: Adjust start_date and end_date to account for multiple output files
    # that require udpated filname{year}.csv formats. And also to incorproate any limitatons
    # that a particular api call my have on how much time-based data it can provide in its returned data set
    
    print("fetch data function running")
    #print(f" api_config['params'] inside fetch_dat () function: {api_config['params']}")
    updated_start_date_str = updated_start_date
    updated_end_date_str = updated_end_date
    #print(f" date_str: {updated_start_date_str} and {updated_end_date_str}")

    # Adjust dates for API calls that require multiple annual files

    # Note that for files that produce multiple annual files, we need to update the date variable
    # as those files are appended with XXXXXX.date.csv. We do this by passing the
    # the "date" varible that is updated within the annual loop that calls this fetch_data() function
    # and we pass it back to the api_config['params'] item in the dictionary.  One caveate here is that
    # some files have a limited time interval (days) that they can report on data. For example some reports only provide
    # data over X days (system marginal price data) or up until X day before current day (e.g. merit order data). To get
    # around this contraint we have added an item to the API Call dictionary stating whether this specific API Call
    # in this loop needs to cap its end date based on limitations.
    
    #Check if data is time series or a list
    if api_config['data_type'] == 'time series':
        #print("Dynamically adusting dates for time series data")

        # Pass updated start date to dictionary Updated start date for year-01-01
        api_config['params']['startDate'] = updated_start_date
        #print(f" Updated Start Date: {updated_start_date}")
        
        # Define updated end date as the yyyy-12-31 or updated start date + X days
        # if api call type does not have any reporting day limits
        if reporting_limit is None:
            # a) 365 days
            # Pass updated end date with yyyy-12-31 format back to dictionary
            print(f" api_config['params'].get('endDate') : {api_config['params'].get('endDate') }")
            if api_config['params'].get('endDate') is not None:
                api_config['params']['endDate'] = updated_end_date
                #print(f" Updated End Date: {updated_end_date}")
        else:
            # b) days determined by reporting limit 
            # Adjust updated end date by adding # days to updated started date
            # First convert the updated starte date string back to to a date
            start_date_dt = datetime.datetime.strptime(updated_start_date, '%Y-%m-%d')
            
            # Then add add 183 days to the converted date object
            updated_end_date = start_date_dt + datetime.timedelta(days=int(reporting_limit))

            # Then convert it back to a string
            updated_end_date_string = updated_end_date.strftime('%Y-%m-%d')
            # Pass truncated and updated end date to dictionary
            if api_config['params']['endDate']:
                api_config['params']['endDate'] = updated_end_date_string
        
    else:
        print("No need to dynamically adust dates for list data")

    #print(f" params: {params}")
    
    #############################################
    # New code to accommodate new AESO API
    # Step #2: Make actual API Call with updated parameters (i.e. dates)
    # old
    # response = requests.get(api_url, headers=headers, params=params)
    
    # new
    # Encode parameters into the URL
    url_with_params = f"{api_url}?{urllib.parse.urlencode(params)}"
    print(f" url_with_params:{url_with_params}")
    
    # Create a request object with the URL and headers
    req = urllib.request.Request(url_with_params, headers=headers)
    print(f"req: {req}")
    
    with urllib.request.urlopen(req) as response:
        # Print the HTTP status code
        response_status_code = response.getcode()
        print(f" response_status_code: {response_status_code}")
        # Read and print the response content
        response_str = response.read().decode('utf-8')
        #response_str = response.read()
        print(f" response_str: {response_str}")
        # print(f"data:{data}")

###################################
    #new code
    try:
        #if response.status_code in [200, 400, 401, 403, 404, 405, 500, 503]:
        if response.getcode() in [200, 400, 401, 403, 404, 405, 500, 503]:
            df = handle_status_code(api_config, response_status_code, response_str)
            #df = handle_status_code(response_str)
            return df
        else:
            print(f"API request failed. Status Code: {response_status_code}, Reason: {response_str}")
            return None
        
    except requests.Timeout:
        print("Request timed out. Consider adjusting the timeout value.")
        return None
    
    except requests.RequestException as e:
        print(f"Request failed: {str(e)}")
        return None
    #############################################
    
    
###############################################
# Helper Functions
###############################################
#------------------------------------------------------

def convert_columns_to_uppercase(df):
    df.columns = [col.upper() for col in df.columns]
    return df
#------------------------------------------------------
def process_unique_asset_ids(df):
    # Step 1: Create a unique list of asset_ID
    unique_asset_ids = df['asset_ID'].unique()
    print("Unique Asset IDs:")
    print(unique_asset_ids)

    # Step 2: Create a list of unique asset_class entries
    unique_asset_classes = df['asset_class'].unique()
    print("\nUnique Asset Classes:")
    print(unique_asset_classes)
    return unique_asset_ids, unique_asset_classes 
#------------------------------------------------------
def reshape_data(df):
    # Use pivot_table to get the desired format
    pivoted_df = df.pivot_table(index=['begin_date_utc', 'begin_date_mpt'], 
                                columns='asset_ID', 
                                values='metered_volume', 
                                aggfunc='first').reset_index()
    
    return pivoted_df
#------------------------------------------------------
def remove_filename(path):
    return os.path.dirname(path) 
#------------------------------------------------------
def get_last_folder(path):
    return os.path.basename(os.path.normpath(path))
#------------------------------------------------------
def consolidate_generation_files(path):
    # This function consolidates the IPP.csv and GENCO.csv files into a single file BY
    # reading the individal files that have already been saved in the output folder
    #Create file Path
    #Use existing path and take out the .csv file already in it
    reduced_path = remove_filename(path)
    sub_folder =  get_last_folder(reduced_path)
    # Get the path without the sub_folder
    path_without_subfolder = os.path.dirname(reduced_path)
    

    # Step 1: Read the IPP.csv file
    filename = 'IPP.csv'
    new_path = create_path(path_without_subfolder,sub_folder, filename)
    new_path = new_path.replace("\\", "/")  
    #ipp_df = pd.read_csv(f"{output_folder}IPP.csv")
    ipp_df = pd.read_csv(new_path)

    # Step 2: Remove columns that match the pattern G###
    columns_to_drop = [col for col in ipp_df.columns if col.startswith('G') and len(col) == 4 and col[1:].isdigit()]
    ipp_df.drop(columns=columns_to_drop, inplace=True)

    # Step 3: Read the GENCO.csv file and append it to the ipp_df, excluding the first two columns
    filename = 'GENCO.csv'
    new_path = create_path(path_without_subfolder,sub_folder, filename)
    new_path = new_path.replace("\\", "/")  
    #genco_df = pd.read_csv(f"{output_folder}GENCO.csv")
    genco_df = pd.read_csv(new_path)
    genco_df.drop(columns=['begin_date_utc', 'begin_date_mpt'], inplace=True)
    
    # Ensure both dataframes are aligned on their index before concatenating
    combined_df = pd.concat([ipp_df, genco_df], axis=1)

    # Step 4: Save the combined DataFrame to "Consolidated Generation Metered Volumes.csv"
    combined_filename = "Consolidated Generation Metered Volumes.csv"
    new_path = create_path(path_without_subfolder,sub_folder, combined_filename)
    save_dataframe_to_csv(combined_df, new_path) 
    new_path = new_path.replace("\\", "/")  
    save_dataframe_to_csv(combined_df, new_path) 

    return
#------------------------------------------------------
def consolidate_annual_files(api_config, output_folder, output_consolidated_csv_files, sub_folder_template, csv_output, \
                                        sqlite_output, conn, db_table_name, column_order):
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
    if csv_output:
        #save_dataframe_to_csv(df, path)

        # Define the directory containing the CSV files
        directory = r'C:\Users\kaczanor\OneDrive - Enbridge Inc\Documents\Python\Revised-AESO-API-master\output\Spot_Prices'
        
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
        merged_df.set_index('begin_datetime_mpt', inplace=True, drop = False)

        # Get the start and end years
        start_year = min(years)
        end_year = max(years)

        # Save the merged DataFrame to a new CSV file with the start and end years in the filename
        #output_file = os.path.join(directory, f'merged_pool_price_data_{start_year}_to_{end_year}.csv')
        #merged_df.to_csv(output_file)

        #print(f"Merged data saved to {output_file}")


        path = f'{output_folder}Spot_Prices/merged_pool_price_data_{start_year}_to_{end_year}.csv'
        #sub_folder_template = "Historical Prices"
        #path = create_path(output_folder, sub_folder_template, file_name_template)

        save_dataframe_to_csv(merged_df, path) 
        print(f"Consolidated data saved to {path}")

    return
#------------------------------------------------------
def consolidate_import_export_files(path):
    reduced_path = remove_filename(path)
    sub_folder =  get_last_folder(reduced_path)
    # Get the path without the sub_folder
    path_without_subfolder = os.path.dirname(reduced_path)
    
    # Step 1: Read the EXPORTER.csv file
    filename = 'EXPORTER.csv'
    new_path = create_path(path_without_subfolder,sub_folder, filename)
    new_path = new_path.replace("\\", "/")  
    exp_df = pd.read_csv(new_path)

    # Step 3: Read the IMPORTER.csv file and append it to the ipp_df, excluding the first two columns
    filename = 'IMPORTER.csv'
    new_path = create_path(path_without_subfolder,sub_folder, filename)
    new_path = new_path.replace("\\", "/")  
    imp_df = pd.read_csv(new_path)
    imp_df.drop(columns=['begin_date_utc', 'begin_date_mpt'], inplace=True)
    
    # Ensure both dataframes are aligned on their index before concatenating
    combined_df = pd.concat([exp_df, imp_df], axis=1)

    # Step 4: Save the combined DataFrame to "Consolidated Generation Metered Volumes.csv"
    combined_filename = "Import_Export.csv"
    new_path = create_path(path_without_subfolder,sub_folder, combined_filename)
    save_dataframe_to_csv(combined_df, new_path) 
    new_path = new_path.replace("\\", "/")  
    save_dataframe_to_csv(combined_df, new_path) 
    
    return
#------------------------------------------------------
def extract_region(asset_name):
    '''
    Unfortunately there is a lot of variation in the Asset Names that we have to extract the
    region from. Note its the Asset Name not the Asset ID that we are extracting from.  
    We need to extract all variations.

    ###################
    BC import/Export Syntax
    Import from BCH
    BC Import
    Import from BCH
    Import from BC

    BC Export
    Export to BC
    BC EXPORT
    Export to BCH
    ###################
    SK import/Export Syntax
    SK Import
    Import from SPC
    Import from Sask
    Import Sask

    SK Export
    Sask Export
    Export to SPC
    Export to Sask
    ###########################
    MT import/Export Syntax
    MT Export
    MT EXPORT
        '''
    asset_name = asset_name.upper()  # Convert to uppercase for case-insensitive matching
    
    # Check for BC region
    if re.search(r'\bIMPORT.*\bBC\b|\bBC\b.*\bIMPORT|\bBC\b', asset_name):
        return 'IMPORT_BC'
    elif re.search(r'\bEXPORT.*\bBC\b|\bBC\b.*\bEXPORT|\bBC\b', asset_name):
        return 'EXPORT_BC'
    
    # Check for SK region
    elif re.search(r'\bIMPORT.*\bSK\b|\bSK\b.*\bIMPORT|\bSPC\b|\bSASK\b', asset_name):
        return 'IMPORT_SK'
    elif re.search(r'\bEXPORT.*\bSK\b|\bSK\b.*\bEXPORT|\bSPC\b|\bSASK\b', asset_name):
        return 'EXPORT_SK'
    
    # Check for MT region
    elif re.search(r'\bIMPORT.*\bMT\b|\bMT\b.*\bIMPORT', asset_name):
        return 'IMPORT_MT'
    elif re.search(r'\bEXPORT.*\bMT\b|\bMT\b.*\bEXPORT', asset_name):
        return 'EXPORT_MT'
    
     # Handle specific phrase 'EXPORT to BCH'
    elif re.search(r'\bEXPORT.*\bTO\b.*\bBCH\b', asset_name):
        return 'EXPORT_BC'
    
    # Default case
    else:
        return None
#------------------------------------------------------
def save_region_mapping_to_csv(region_mapping, output_file_path):
    # Define the header for the CSV file
    header = ['ASSET_ID', 'REGION', 'ASSET_TYPE', 'POOL_ID']

    # Open the output file in write mode
    with open(output_file_path, mode='w', newline='') as file:
        writer = csv.writer(file)

        # Write the header
        writer.writerow(header)

        # Write the data
        for asset_id, (asset_name, region, asset_type, pool_id) in region_mapping.items():
            writer.writerow([asset_id, region, asset_type, pool_id])
#------------------------------------------------------
# Define a function to determine the REGION based on the given conditions
def determine_region(row):
    asset_type = row['ASSET_TYPE']
    asset_name = row['ASSET_NAME']
    asset_id = row['ASSET_ID']
    
    # Export asset
    if asset_type == 'SINK':

        if 'BC' in asset_name:
            return 'EXPORT_BC'
        elif 'MT' in asset_name:
            return 'EXPORT_MT'
        else:
            return 'EXPORT_SK'
    elif asset_type == 'SOURCE':
        # Import asset
        if 'BC' in asset_name:
            return 'IMPORT_BC'
        elif 'MT' in asset_name:
            return 'IMPORT_MT'
        else:
            return 'IMPORT_SK'
    return None

#------------------------------------------------------
def read_import_export_map(file_path):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(file_path)

    # Filter rows that have "import" or "export" in the ASSET_NAME column (case insensitive)
    filtered_df = df[df['OPERATING_STATUS'] == 'Active']
    filtered_df = filtered_df[filtered_df['ASSET_NAME'].str.contains('import|export', case=False, na=False)]

    # Initialize an empty dictionary for region mapping
    region_mapping = {}

    # Initialize new REGION column to create
    filtered_df['REGION'] = None

    # Iterate over rows in the DataFrame to populate region_mapping
    for _, row in filtered_df.iterrows():
        asset_id = row['ASSET_ID']
        asset_name = row['ASSET_NAME'] #New
        asset_type = row['ASSET_TYPE']
        pool_id = row['POOL_PARTICIPANT_ID']

        #For the REGION column we will have to extract data to create it
        #This needs to be the column with IMPORT_ or EXPORT_
        row['REGION'] = determine_region(row)
        region = row['REGION']

        region_mapping[asset_id] = (asset_name, region, asset_type, pool_id)
        print(f"Asset ID: {asset_id}, Asset Name: {asset_name}, Region: {region}, Asset Type: {asset_type}, Pool ID: {pool_id}")

    output_file_path = r'C:\Users\kaczanor\OneDrive - Enbridge Inc\Documents\Python\Revised-AESO-API-master\object_mapping\Region_Mapping.csv'
    save_region_mapping_to_csv(region_mapping, output_file_path)

    return region_mapping
#------------------------------------------------------
def find_length_shape_of_data_frames(*named_data_frames):
    for name, df in named_data_frames:
        # Check length of the DataFrame
        length_of_data = len(df)
        print(f"Length of data frame '{name}': {length_of_data}")

        # Check shape of the DataFrame
        shape_of_data = df.shape
        print(f"Shape of data frame '{name}': {shape_of_data}")
#-----------------------------------------------------
def create_complete_date_range(start_date_str, end_date_str):
    start_date = pd.to_datetime(start_date_str)
    end_date = pd.to_datetime(end_date_str)
    return pd.date_range(start=start_date, end=end_date, freq='H')
#-----------------------------------------------------
def check_missing_dates(df, date_col, complete_date_range, df_name):
    missing_dates = complete_date_range.difference(df[date_col].dropna())
    if not missing_dates.empty:
        print(f"Missing dates in {df_name} ({date_col}): {missing_dates}")
    else:
        print(f"No missing dates in {df_name} ({date_col})")
#-----------------------------------------------------
def create_regional_import_export_file(path, updated_start_date, original_end_date):
    #This converts the import/export data by asset id into specific tie lines
    print(f" start_date and end_date data types: {type(updated_start_date)} and {type(original_end_date)}")
    print(f" start_date: {updated_start_date}")
    print(f" end_date: {original_end_date}")

    

    #New
    updated_start_date_str = updated_start_date
    original_end_date_str = original_end_date
    complete_date_range = create_complete_date_range(updated_start_date_str, original_end_date_str)

    original_end_date = pd.to_datetime(original_end_date)
    # Have to convert start_date to date from date-time
    current_date = updated_start_date.date()
    original_end_date = original_end_date.date()
    
    file_year_suffix = int(current_date.year)

    all_data = []
    
    #######################################
    #Import all data files before loop
    ######################################
    #Load the Asset List File and it is need to combine meta data from the Asset List and the Import/Export data
    asset_list = pd.read_csv(r'C:\Users\kaczanor\OneDrive - Enbridge Inc\Documents\Python\Revised-AESO-API-master\output\Asset List\Asset_Lists.csv')
    print(f" asset_list: {asset_list}")
    # Create Import Export File by filtering on the Asset IDs for 


    # Import the Master File for Import/Export Mapping
    # This is a Master file which has more data than we need.
    # We will filter it down to the data we need 
    # Call the function to get the region mapping dictionary
    # !!!! Where does this file come from?
    region_mapping = read_import_export_map(r'C:\Users\kaczanor\OneDrive - Enbridge Inc\Documents\Python\Revised-AESO-API-master\object_mapping\Import_Export_Map.csv')
    print(f" region_mapping: {region_mapping}")

    #####################################
    # Load the Importer and Exporter data files as it is these files that we are going to:
    # 1) Map the Asset ID to the Region, and
    # 2) Aggregate the imports and exports by 3x regions/lines (BC, SK, MT)
    import_data = pd.read_csv(r'C:\Users\kaczanor\OneDrive - Enbridge Inc\Documents\Python\Revised-AESO-API-master\output\Metered Volumes\IMPORTER.csv')
    print(f"import_data: {import_data.head()}")

    export_data = pd.read_csv(r'C:\Users\kaczanor\OneDrive - Enbridge Inc\Documents\Python\Revised-AESO-API-master\output\Metered Volumes\EXPORTER.csv')
    print(f"export_data: {export_data.head()}")
    
    # Convert date columns to datetime
    import_data['begin_date_mpt'] = pd.to_datetime(import_data['begin_date_mpt'])
    export_data['begin_date_mpt'] = pd.to_datetime(export_data['begin_date_mpt'])

     # Create a complete date range from updated_start_date to original_end_date
    complete_date_range = pd.date_range(start=updated_start_date, end=original_end_date, freq='H')

    # Identify missing dates in import data
    missing_import_dates = complete_date_range.difference(import_data['begin_date_mpt'])
    print(f"Missing dates in import data: {missing_import_dates}")

    # Identify missing dates in export data
    # missing_export_dates = complete_date_range.difference(export_data['begin_date_mpt'])
    # print(f"Missing dates in export data: {missing_export_dates}")

    #Check Length of Import and Export Data
    print("Initial Check on Length and Shape of Data Frames")
    find_length_shape_of_data_frames(
    ("import_data", import_data), 
    ("export_data", export_data)
)

    # Check for missing dates in initial data
    check_missing_dates(import_data, 'begin_date_mpt', complete_date_range, 'import_data')
    check_missing_dates(export_data, 'begin_date_mpt', complete_date_range, 'export_data')

    #####################################
    # Load the Importer and Exporter data fil

    # Define the mapping from Type to route columns
    route_mapping = {
            'IMPORT_BC': 'IMPORT_BC',
            'IMPORT_MT': 'IMPORT_MT',
            'IMPORT_SK': 'IMPORT_SK',
            'EXPORT_BC': 'EXPORT_BC',
            'EXPORT_MT': 'EXPORT_MT',
            'EXPORT_SK': 'EXPORT_SK'
        }
        
    # Define import and export route columns
    import_routes = ['IMPORT_BC', 'IMPORT_MT', 'IMPORT_SK']
    export_routes = ['EXPORT_BC', 'EXPORT_MT', 'EXPORT_SK']

    #######################################
    # Part A: Create a IMPORT_EXPORT_MAP in the Metered Volume folder which will be used to categorize the 
    # the imports and exports into buckets.  This involves loading the asset list, filtering it, to only include
    # the active assets, then filtering it to only include the source and sink assets, and then filtering it to only
    # include the assets that have 'Import' or 'Export' in the asset name.  The final step is to create a new column
    # that will contain the REGION, ASSET_TYPE, and POOL_ID extracted from the asset ID based on the region_mapping dictionary.
    # The REGION column will be one of the following: 'IMPORT_BC', 'IMPORT_MT', 'IMPORT_SK', 'EXPORT_BC', 'EXPORT_MT', 'EXPORT_SK'.
    # The ASSET_TYPE column will be 'SOURCE' or 'SINK', and the POOL_PARTICIPANT_ID column will contain the pool participant ID.
    # The REGION values will be extracted from the asset ID based on the region_mapping dictionary.
    #######################################
    # Load Asset_List.csv
    print(f"Asset List: {asset_list.head()}")
    print(f"Asset List.columns: {asset_list.columns}")
    #--------------------------------------------------------
    # Step 1: Inspect the unique values in 'operating_status' column to understand its content
    #--------------------------------------------------------
    print(f"Unique operating statuses: {asset_list['OPERATING_STATUS'].unique()}")

    #--------------------------------------------------------
    # Step 2: Filter on 'Active' in asset_type
    #--------------------------------------------------------
    # Convert both sides to upper case and strip leading/trailing whitespace for robust comparison
    # Normalization: Use str.strip().str.upper() to normalize strings before comparison to ensure consistent 
    # handling of case and whitespace.
    filtered_assets = asset_list[asset_list['OPERATING_STATUS'] == 'Active']
    print(f"Filtered assets (Active): {filtered_assets.head()}")
    #--------------------------------------------------------
    # Step 3: Filter on 'Source' or 'Sink' in asset_type
    #--------------------------------------------------------
    filtered_assets = asset_list[asset_list['ASSET_TYPE'].str.upper().isin(['SOURCE', 'SINK'])]
    print(f"filtered_assets source/sink: {filtered_assets.head()}")
    #--------------------------------------------------------
    # Step 4: Filter asset_name for entries containing 'Import' and 'Export'
    #--------------------------------------------------------
    filtered_assets['ASSET_NAME'] = filtered_assets['ASSET_NAME'].apply(lambda x: x.upper())
    filtered_assets = filtered_assets[filtered_assets['ASSET_NAME'].str.contains('IMPORT|EXPORT', case=False)]
    print(f"filtered_assets.columns: {filtered_assets.columns}")
    print(f"filtered_assets import and exports: {filtered_assets.head()}")



    #--------------------------------------------------------
    # Step 5: Create Import/Export column
    #--------------------------------------------------------
    # Function to extract region, type, and pool ID from Asset ID based on the region_mapping
    def extract_region_type_pool(asset_id):
        try:
            return region_mapping[asset_id]
        except KeyError:
            print(f"No mapping found for asset ID '{asset_id}'")
            return None, None, None, None  # Return default values or handle as needed

    try:
        # Note the order of the columns in the zip function must match the order of the columns in the filtered_assets dataframe
        # The str.strp() function is used to remove leading and trailing whitespaces from the asset_ID column
        filtered_assets['ASSET_NAME'], filtered_assets['REGION'], filtered_assets['ASSET_TYPE'], filtered_assets['POOL_PARTICIPANT_ID'] = \
                    zip(*filtered_assets['ASSET_ID'].str.strip().map(extract_region_type_pool))
        
        print(f"filtered_assets: {filtered_assets.head()}")
    except AttributeError as e:
        if "strip" in str(e):
            print("AttributeError: 'asset_ID' column does not support strip operation. Check for non-string values.")
        else:
            print(f"AttributeError: {e}")
    except Exception as e:
        print(f"Error: {e}")

    print(f"filtered_assets: {filtered_assets.head(100)}")
    
    #--------------------------------------------------------
    # Step 6: Final review for missing REGION fields
    #--------------------------------------------------------
    missing_regions = filtered_assets[filtered_assets['REGION'].isnull()]
    if not missing_regions.empty:
        print(f"Warning: There are missing REGION fields for the following asset IDs: {missing_regions['ASSET_ID'].tolist()}")

    #--------------------------------------------------------
    # Step 7: Save the refined and filtered IMPORT_EXPORT_MAP.csv back to the Metered Volume Folder
    #--------------------------------------------------------

    reduced_path = remove_filename(path)
    sub_folder =  get_last_folder(reduced_path)
    path_without_subfolder = os.path.dirname(reduced_path)
    new_path = create_path(path_without_subfolder,sub_folder, 'IMPORT_EXPORT_MAP.csv')
    new_path = new_path.replace("\\", "/") 
    save_dataframe_to_csv(filtered_assets, new_path) 

    #######################################
    # Part B: Create Categorized IMPORT.csv and EXPORT.csv file
    #######################################
    #--------------------------------------------------------
    #Step 8: Load and Reshape import_data and export_data
    #--------------------------------------------------------
  
     # Merge IMPORT.csv and EXPORT.csv with import_export_map
     # The melt function is used to transform or reshape data. It converts data from a wide format 
     # (where you have multiple columns representing different variables) to a long format (where you have fewer columns, but more rows).
     # This specifies the columns that should remain as identifier variables. In this case, 'begin_date_utc' and 'begin_date_mpt' are the 
     # identifier columns that will stay the same in each row of the reshaped DataFrame.  
     # var_name is specifies the name of the new column that will contain the variable names (formerly the column headers for the asset volumes). 
     # In this case, the new column name will be 'ASSET_ID'.
     # value_name parameter: This specifies the name of the new column that will contain the values (formerly the data in the asset volume columns). 
     # In this case, the new column name will be TOTAL_IMPORTS OR TOTAL_EXPORTS
     # Assume import_data looks like this:
    '''
    begin_date_utc	begin_date_mpt	Asset_A	Asset_B	Asset_C
    2023-01-01	    2023-01-01	    100	    200	    300
    2023-01-02	    2023-01-02	    110	    210	    310

    After applying the melt function, import_data_long will look like this:

    begin_date_utc	begin_date_mpt	ASSET_ID	TOTAL_IMPORTS (OR TOTAL_EXPORTS)
    2023-01-01	    2023-01-01	    Asset_A	    100
    2023-01-01	    2023-01-01	    Asset_B	    200
    2023-01-01	    2023-01-01	    Asset_C	    300
    2023-01-02	    2023-01-02	    Asset_A	    110
    2023-01-02	    2023-01-02	    Asset_B	    210
    2023-01-02	    2023-01-02	    Asset_C	    310

    '''

    import_data_long = import_data.melt(id_vars=['begin_date_utc', 'begin_date_mpt'], var_name='ASSET_ID', value_name='TOTAL_IMPORTS')
    export_data_long = export_data.melt(id_vars=['begin_date_utc', 'begin_date_mpt'], var_name='ASSET_ID', value_name='TOTAL_EXPORTS')

    #Check Length of Import and Export Data
    print("Second Check on Length and Shape of Data Frames")
    #find_length_shape_of_data_frames(import_data_long, export_data_long)
    find_length_shape_of_data_frames(
    ("import_data_long", import_data_long), 
    ("export_data_long", export_data_long)
    )

    # Check for missing dates after melting
    check_missing_dates(import_data_long, 'begin_date_mpt', complete_date_range, 'import_data_long')
    check_missing_dates(export_data_long, 'begin_date_mpt', complete_date_range, 'export_data_long')


    #Check for any non-null values
    import_data_long_valid_volume = import_data_long[(import_data_long['TOTAL_IMPORTS'].notna()) & (import_data_long['TOTAL_IMPORTS'] != 0)]
    export_data_long_valid_volume = export_data_long[(export_data_long['TOTAL_EXPORTS'].notna()) & (export_data_long['TOTAL_EXPORTS'] != 0)]
    print(import_data_long_valid_volume)
    print(export_data_long_valid_volume)

    # Check for any non-null values
    imports_data_long_has_values = import_data_long['TOTAL_IMPORTS'].notna().any()
    exports_data_long_has_values = export_data_long['TOTAL_EXPORTS'].notna().any()
    print("Imports any non-null values:", imports_data_long_has_values)
    print("Exports any non-null values:", exports_data_long_has_values)

    # Check for any non-zero values
    imports_data_long_has_non_zero_values = (import_data_long['TOTAL_IMPORTS'] != 0).any()
    exports_data_long_has_non_zero_values = (export_data_long['TOTAL_EXPORTS'] != 0).any()
    print("Imports any non-zero values:", imports_data_long_has_non_zero_values)
    print("Exports any non-zero values:", exports_data_long_has_non_zero_values)

    # Check for any null values
    imports_data_long_has_null_values = import_data_long['TOTAL_IMPORTS'].isna().any()
    exports_data_long_has_null_values = export_data_long['TOTAL_EXPORTS'].isna().any()
    print("Imports any null values:", imports_data_long_has_null_values)
    print("Exports any null values:", exports_data_long_has_null_values)

    # Count the non-null values
    imports_data_long_non_null_count = import_data_long['TOTAL_IMPORTS'].count()
    exports_data_long_non_null_count = export_data_long['TOTAL_EXPORTS'].count()
    print("Imports number of non-null values:", imports_data_long_non_null_count)
    print("Exports number of non-null values:", exports_data_long_non_null_count)

    # Check if the column is empty
    imports_data_long_is_empty = import_data_long['TOTAL_IMPORTS'].empty
    exports_data_long_is_empty = export_data_long['TOTAL_EXPORTS'].empty
    print("Imports is the column empty:", imports_data_long_is_empty)
    print("Exports is the column empty:", exports_data_long_is_empty)

    print("Import Data (Long Format):")
    print(import_data_long.head())
    
    print("\nExport Data (Long Format):")
    print(export_data_long.head())

    #Check Length of Import and Export Data
    print("Third Check on Length and Shape of Data Frames")
    #find_length_shape_of_data_frames(import_data_long, export_data_long) 
    find_length_shape_of_data_frames(
    ("import_data_long", import_data_long), 
    ("export_data_long", export_data_long)
)
  
    # Check for missing dates after categorization
    check_missing_dates(import_data_long, 'begin_date_mpt', complete_date_range, 'import_data_long')
    check_missing_dates(export_data_long, 'begin_date_mpt', complete_date_range, 'export_data_long')

    #--------------------------------------------------------
    # Step 9: Prepare import_export_map for merge
    #--------------------------------------------------------
    # Merge import_data_long and export_data_long with import_export_map
    # pd.merge Function: This function is used to combine two DataFrames based on a common key.
    # 1) how='left': The type of merge being performed is a left join. This means all rows from the left DataFrame 
    #    (import_data_long and export_data_long) will be kept, and matching rows from the right DataFrame (import_export_map) 
    #    will be added. If there is no match, the result will contain NaN for those columns from the right DataFrame.
    # 2) left_on='ASSET_ID' and right_on='ASSET_ID': Specifies the columns to join on. The ASSET_ID column in the left 
    #    DataFrame is matched with the ASSET_ID column in the right DataFrame.

    #import_export_map = pd.read_csv(r'C:\Users\kaczanor\OneDrive - Enbridge Inc\Documents\Python\Revised-AESO-API-master\Import_Export_Map.csv')
    import_export_map = pd.read_csv(r'C:\Users\kaczanor\OneDrive - Enbridge Inc\Documents\Python\Revised-AESO-API-master\output\Metered Volumes\IMPORT_EXPORT_MAP.csv')

    '''
    At this point your data will look like this for import_data_long:

       begin_date_utc    begin_date_mpt       ASSET_ID  TOTAL_IMPORTS
    0  2010-01-01 07:00  2010-01-01 00:00     AQBC      0.0

    And your import.export map will look like this:
    Unnamed: 0, UNNAMED: 0, ASSET_NAME,         ASSET_ID,    ASSET_TYPE,    OPERATING_STATUS,       POOL_PARTICIPANT_NAME, POOL_PARTICIPANT_ID, NET_TO_GRID_ASSET_FLAG, ASSET_INCL_STORAGE_FLAG, REGION
    89,         89,         ABCP APC BC IMPORT, ABCP,SOURCE, Active,        Heartland Generation 1, APC                    ,                    ,                       ,                        EXPORT_BC
    93,         93,         ABSK APC SK IMPORT, ABSK,SOURCE, Active,        Heartland Generation 1, APC                    ,                    ,                       ,                        EXPORT_SK
    '''
    import_categorized = pd.merge(import_data_long, import_export_map, how='left', left_on='ASSET_ID', right_on='ASSET_ID')
    export_categorized = pd.merge(export_data_long, import_export_map, how='left', left_on='ASSET_ID', right_on='ASSET_ID')
    
    #Check Length of Import and Export Data
    print("Fourth Check on Length and Shape of Data Frames")
    #find_length_shape_of_data_frames(import_categorized, export_categorized)
    find_length_shape_of_data_frames(
    ("import_categorized", import_categorized), 
    ("export_categorized", export_categorized)
)

    # Check for missing dates after categorization
    check_missing_dates(import_categorized, 'begin_date_mpt', complete_date_range, 'import_categorized')
    check_missing_dates(export_categorized, 'begin_date_mpt', complete_date_range, 'export_categorized')

    #Check for any non-null values
    # New
    # Apply filters for non-null and non-zero values
    import_categorized_filtered = import_categorized[(import_categorized['TOTAL_IMPORTS'].notna()) & (import_categorized['TOTAL_IMPORTS'] != 0)]
    export_categorized_filtered = export_categorized[(export_categorized['TOTAL_EXPORTS'].notna()) & (export_categorized['TOTAL_EXPORTS'] != 0)]
    
    print(f" import_categorized: {import_categorized_filtered}")
    print(f" export_categorized: {export_categorized_filtered}")


    #Do prelimary check on column names
    print(f"import_categorized_filtered.columns: {import_categorized_filtered.columns}")
    print(f"export_categorized_filtered.columns: {export_categorized_filtered.columns}")

    # Remove any unnamed columns
    # Regular Expression Modification: The str.contains('(?i)Unnamed') pattern is case-insensitive due to the (?i) prefix. 
    # This will match any column names containing 'Unnamed' regardless of the case.

    #New referneces import_categorized_filtered and export_categorized_filtered
    import_categorized_filtered = import_categorized_filtered.loc[:, ~import_categorized_filtered.columns.str.contains('(?i)Unnamed')]
    export_categorized_filtered = export_categorized_filtered.loc[:, ~export_categorized_filtered.columns.str.contains('(?i)Unnamed')]

    #Check Length of Import and Export Data
    print("Fifth Check on Length and Shape of Data Frames")

    #NEW
    find_length_shape_of_data_frames(
    ("import_categorized_filtered", import_categorized_filtered), 
    ("export_categorized_filtered", export_categorized_filtered)
)

    #new
    # Check for missing dates after filtering
    check_missing_dates(import_categorized_filtered, 'begin_date_mpt', complete_date_range, 'import_categorized_filtered')
    check_missing_dates(export_categorized_filtered, 'begin_date_mpt', complete_date_range, 'export_categorized_filtered')

    # Verify the Cleanup: Print the column names again to ensure unwanted columns are removed.
    print(f"Cleaned import_categorized.columns: {import_categorized_filtered.columns}")
    print(f"Cleaned export_categorized.columns: {export_categorized_filtered.columns}")

    print("Import Data Categorized:")
    print(import_categorized_filtered.head())
    print("\nExport Data Categorized:")
    print(export_categorized_filtered.head())

    # Convert to date time
    import_categorized_filtered['begin_date_mpt'] = pd.to_datetime(import_categorized_filtered['begin_date_mpt'],'%m/%d/%Y %H:%M')
    export_categorized_filtered['begin_date_mpt'] = pd.to_datetime(export_categorized_filtered['begin_date_mpt'],'%m/%d/%Y %H:%M')

    # Ensure datetime conversion
    print("\nImport Data with DateTime Conversion:")
    print(import_categorized_filtered)
    print("\nExport Data with DateTime Conversion:")
    print(export_categorized_filtered)

    # Sort Data
    import_categorized_filtered_sorted = import_categorized_filtered.sort_values(by=['begin_date_mpt', 'begin_date_utc'], inplace=False)
    export_categorized_filtered_sorted = export_categorized_filtered.sort_values(by=['begin_date_mpt', 'begin_date_utc'], inplace=False)

    import_categorized_filtered_sorted.set_index('begin_date_mpt', inplace=True, drop=False)
    export_categorized_filtered_sorted.set_index('begin_date_mpt', inplace=True, drop=False)

    # Ensure sorting
    print("\nSorted Import Data:")
    print(import_categorized_filtered_sorted)
    print("\nSorted Export Data:")
    print(export_categorized_filtered_sorted)

    ####################################
    #Step 10: Save filtered data into temp folder
    ####################################
    '''
    Now the import/export data has tie lines attached to each transaction:
    begin_date_utc,begin_date_mpt,ASSET_ID,TOTAL_EXPORTS,ASSET_NAME,ASSET_TYPE,OPERATING_STATUS,POOL_PARTICIPANT_NAME,POOL_PARTICIPANT_ID,NET_TO_GRID_ASSET_FLAG,ASSET_INCL_STORAGE_FLAG,REGION
    2024-01-01 07:00,2024-01-01 00:00:00,PW20,935.0,PW20 PWX EXPORT TO BCH,SINK,Active,Powerex Corp.,PWX,,,EXPORT_BC
    
    '''
    path = r'C:\Users\kaczanor\OneDrive - Enbridge Inc\Documents\Python\Revised-AESO-API-master\output'
    sub_folder =  "temp"
    new_path1 = create_path(path,sub_folder, f'import_categorized_filtered_sorted_{file_year_suffix}.csv')
    new_path1 = new_path1.replace("\\", "/") 
    print(f"new_path1: {new_path1}")
    save_dataframe_to_csv(import_categorized_filtered_sorted, new_path1)

    path = r'C:\Users\kaczanor\OneDrive - Enbridge Inc\Documents\Python\Revised-AESO-API-master\output'
    sub_folder =  "temp"
    new_path2 = create_path(path,sub_folder, f'export_categorized_filtered_sorted_{file_year_suffix}.csv')
    new_path2 = new_path2.replace("\\", "/") 
    print(f"new_path2: {new_path2}")
    save_dataframe_to_csv(export_categorized_filtered_sorted, new_path2) 

    #--------------------------------------------------------
    # Step 10: Aggregate data by route
    # Function to map Type to Route columns
    # This passes the row of the data frame and the list of route mapping to the fnction
    # the route mappings are 
    # ['IMPORT_BC', 'IMPORT_MT', 'IMPORT_SK','EXPORT_BC', 'EXPORT_MT', 'EXPORT_SK']
    def map_to_route(row, route_mapping, total_column):
        #print(f"Processing hour: {row['begin_date_mpt']}, row: {row['ASSET_ID']}, Type: {row['ASSET_TYPE']}, Volume: {row[total_column]}")
        # Only proceed with non null values in the ASSET_TYPE column
        # These values will be either 'SOURCE' or 'SINK'
        if pd.notnull(row['ASSET_TYPE']):
            #print(f"row['ASSET_TYPE']: {row['ASSET_TYPE']}")
            pass
            # Get the route based on the ASSET_TYPE column 'SOURCE/SINK'
            route = route_mapping.get(row['REGION'])
            #print(f"Route: {route}")
            # Assign the volume to the corresponding route column
            if route:
                row[route] = row[total_column]  # Assuming 'Volume' is the relevant data to aggregate
                #print(f"Assigned Volume: {row[total_column]} to Route: {route} for Asset ID: {row['ASSET_ID']}")
            else:
                #print(f"No route mapping found for ASSET_TYPE: {row['ASSET_TYPE']}")
                pass
        return row

    for route in import_routes + export_routes:
        import_categorized[route] = 0
        export_categorized[route] = 0

    # Function to process data in batches as the data set is large and creates memory issues
    def process_in_batches(df, route_mapping, total_column, batch_size=1000):
        num_batches = (len(df) // batch_size) + 1
        df_list = []
        for i in tqdm(range(num_batches), desc="Processing in batches"):
            start_idx = i * batch_size
            end_idx = start_idx + batch_size
            batch = df.iloc[start_idx:end_idx].copy()  # Use .copy() to avoid SettingWithCopyWarning
            batch = batch.apply(lambda row: map_to_route(row, route_mapping, total_column), axis=1)
            df_list.append(batch)
        return pd.concat(df_list, ignore_index=True)

    # iterates through each route name stored in import_routes and export_routes. These are lists containing 
    # names of different import and export routes. For each route name (route), it sets the corresponding column 
    # in both import_categorized and export_categorized DataFrames to 0. This initializes these columns to zero 
    # before they are populated with aggregated values in subsequent steps. This ensures that all routes have a 
    # starting point of zero before aggregation, preventing issues with uninitialized or NaN values during aggregation operations.
    
    tqdm.pandas(desc="Processing import routes")
    # Apply map_to_route function to each row
    total_column = 'TOTAL_IMPORTS'
    import_categorized = process_in_batches(import_categorized, route_mapping, total_column)
    print(f"import_categorized: {import_categorized.head(100)}")
    
    tqdm.pandas(desc="Processing export routes")
    total_column = 'TOTAL_EXPORTS'
    export_categorized = process_in_batches(export_categorized, route_mapping, total_column)
    print(f"export_categorized: {export_categorized.head(100)}")

    #Check Length of Import and Export Data
    print("Sixth Check on Length and Shape of Data Frames")
    #find_length_shape_of_data_frames(import_categorized, export_categorized)
    find_length_shape_of_data_frames(
    ("import_categorized", import_categorized), 
    ("export_categorized", export_categorized)
)
    # Check for missing dates after categorization
    check_missing_dates(import_categorized, 'begin_date_mpt', complete_date_range, 'import_categorized')
    check_missing_dates(export_categorized, 'begin_date_mpt', complete_date_range, 'export_categorized')
    
    try: 
        # Aggregate import data
        print(import_categorized.head(100))
        #import_summary = import_categorized_filtered.groupby(['begin_date_utc', 'begin_date_mpt'])[['IMPORT_BC', 'IMPORT_MT', 'IMPORT_SK']].sum().reset_index()
        import_summary = import_categorized.groupby(['begin_date_utc', 'begin_date_mpt'])[['IMPORT_BC', 'IMPORT_MT', 'IMPORT_SK']].sum().reset_index()
    

        # Calculate TOTAL_IMPORTS as the sum of the individual import categories
        #new
        #import_summary = import_categorized_filtered.groupby(['begin_date_utc', 'begin_date_mpt'])[['TOTAL_IMPORTS']].sum().reset_index()
        import_summary = import_categorized.groupby(['begin_date_utc', 'begin_date_mpt'])[['TOTAL_IMPORTS']].sum().reset_index()
        #print(f"Import Summary:\n{import_summary}")
        #print specific row for the date: 1/1/2010 0:00
        #print(f"Import Summary for 1/1/2010 0:00: {import_summary[import_summary['begin_date_mpt'] == '1/1/2010 0:00']}")

        # Aggregate export data
        #export_summary = export_categorized_filtered.groupby(['begin_date_utc', 'begin_date_mpt'])[['EXPORT_BC', 'EXPORT_MT', 'EXPORT_SK']].sum().reset_index()
        export_summary = export_categorized.groupby(['begin_date_utc', 'begin_date_mpt'])[['EXPORT_BC', 'EXPORT_MT', 'EXPORT_SK']].sum().reset_index()
        
        print(f"Export Summary:\n{export_summary}")
        
        # Calculate TOTAL_EXPORTS as the sum of the individual import categories
        #new
        #export_summary = export_categorized_filtered.groupby(['begin_date_utc', 'begin_date_mpt'])[['TOTAL_EXPORTS']].sum().reset_index()
        export_summary = export_categorized.groupby(['begin_date_utc', 'begin_date_mpt'])[['TOTAL_EXPORTS']].sum().reset_index()

        print(f"Export Summary:\n{export_summary}")
        #print specific row for the date: 1/1/2010 0:00
        print(f"Emport Summary for 1/1/2010 0:00: {export_summary[export_summary['begin_date_mpt'] == '1/1/2010 0:00']}")

        #Check Length of Import and Export Data
        print("Seventh Check on Length and Shape of Data Frames")
        #find_length_shape_of_data_frames(import_summary, export_summary)
        find_length_shape_of_data_frames(
            ("import_summary", import_summary), 
            ("export_summary", export_summary)
        )

        # Check for missing dates after categorization
        check_missing_dates(import_summary, 'begin_date_mpt', complete_date_range, 'import_summary')
        check_missing_dates(export_summary, 'begin_date_mpt', complete_date_range, 'export_summary')

        # Merge import and export summaries on 'begin_date_utc' and 'begin_date_mpt'
        final_summary = pd.merge(import_summary, export_summary, on=['begin_date_utc', 'begin_date_mpt'], how='outer')
        print(f"Final Summary:\n{final_summary}")

        # Fill NaN values with 0 if needed (in case there are missing entries for certain hours)
        final_summary = final_summary.fillna(0)
        
        #Check Length of Import and Export Data
        print("Eighth Check on Length and Shape of Data Frames")
        find_length_shape_of_data_frames(
            ("final_summary", final_summary)
        )

        # Check for missing dates after categorization
        check_missing_dates(final_summary, 'begin_date_mpt', complete_date_range, 'final_summary')

        #Sort Data
        # Convert begin_date_mpt to datetime first
        final_summary['begin_date_utc'] = pd.to_datetime(final_summary['begin_date_utc'], format='%m/%d/%Y %H:%M')
        final_summary['begin_date_mpt'] = pd.to_datetime(final_summary['begin_date_mpt'], format='%m/%d/%Y %H:%M')

        # Handle NaN values in date columns
        final_summary['begin_date_utc'] = final_summary['begin_date_utc'].fillna(pd.Timestamp('1900-01-01 00:00:00'))
        final_summary['begin_date_mpt'] = final_summary['begin_date_mpt'].fillna(pd.Timestamp('1900-01-01 00:00:00'))

        #Check Length of Import and Export Data
        final_summary_sorted = final_summary.sort_values(by=['begin_date_mpt'], ascending=True)
        print(f"Final Summary Sorted:\n{final_summary_sorted}")

    except Exception as e:
        print(f"An error occurred during aggregation: {e}")
    
    ####################################
    #Step 11: Save Data
    ####################################
    '''
    This aggregates the detailed hourly transaction by importer/exporter
    into consosolidated Total Imports and Total Exports

    begin_date_utc,begin_date_mpt,TOTAL_IMPORTS,TOTAL_EXPORTS

    '''


    #!!!!!! Change the save location to the temp folder
    # reduced_path = remove_filename(path)
    # sub_folder =  get_last_folder(reduced_path)
    # path_without_subfolder = os.path.dirname(reduced_path)
    # new_path = create_path(path_without_subfolder,sub_folder, 'aggregated_hourly_import_export_data.csv')
    # new_path = new_path.replace("\\", "/") 
    # save_dataframe_to_csv(final_summary, new_path)

    path = r'C:\Users\kaczanor\OneDrive - Enbridge Inc\Documents\Python\Revised-AESO-API-master\output'
    filename = f'aggregated_hourly_import_export_data_{file_year_suffix}.csv'
    sub_folder =  "temp"
    #new_path = create_path(path_without_subfolder,sub_folder, 'aggregated_hourly_import_export_data.csv')
    #new_path = create_path(path,sub_folder, 'aggregated_hourly_import_export_data.csv')
    new_path = create_path(path,sub_folder, filename)
    new_path = new_path.replace("\\", "/") 
    print(f"new_path: {new_path}")
    save_dataframe_to_csv(final_summary, new_path) 



    print(f" new_path: {new_path}")
    
    return file_year_suffix #new added this as the code was not producing this output for every year.


#################################################
#Post processing functions for each api call
#################################################
def final_processing_pool_participant_data(api_config, df, output_csv_files, updated_start_date, updated_end_date, original_end_date, year, path, csv_output, sqlite_output, conn, table, columns):
                                
    #no post processing required for this api call
    print(f"Processing Pool Particiant Data: {year}")

    if csv_output:
        save_dataframe_to_csv(df, path) 
   
    if sqlite_output:
        save_to_sqlite(df, table, columns, conn)

    print(f"Data saved to {path}")
    return df
 #---------------------------------------------------  
def final_processing_operating_reserve_offer_control_data(api_config, df, output_csv_files, updated_start_date, updated_end_date, original_end_date, year, path, csv_output, sqlite_output, conn, table, columns):
                                
    #no post processing required for this api call
    print(f"Processing Operating Reserves Offer Control Data: {year}")

    if csv_output:
        save_dataframe_to_csv(df, path) 
   
    if sqlite_output:
        save_to_sqlite(df, table, columns, conn)

    print(f"Data saved to {path}")
    return df
 #---------------------------------------------------  

def final_processing_actual_forecast_report_data(api_config, df, output_csv_files, updated_start_date, updated_end_date, original_end_date, year, path, csv_output, sqlite_output, conn, table, columns):
                                
    #no post processing required for this api call
    print(f"Processing Actual Forecast Report Data: {year}")
    
    # Comnbine ail data with export data
    # Requires both ail and metered volume queries to be made

    # Access the public object
    # dict = main.api_function_call_dict
    
    # ail_demand_run_value = dict['OLD_AESO']['AIL_Demand']['run_option']
    # metered_volume_run_value  = dict['OLD_AESO']['Metered_Volume_Data']['run_option']
    
    # if ail_demand_run_value and metered_volume_run_value:
    #     combine_ail_exports_imports()
        
    
    
    if csv_output:
        save_dataframe_to_csv(df, path) 
   
    if sqlite_output:
        save_to_sqlite(df, table, columns, conn)

    print(f"Data saved to {path}")
    return df
 #---------------------------------------------------    
def final_processing_ail_demand_data(api_config, df, output_csv_files, updated_start_date, updated_end_date, original_end_date, year, path, csv_output, sqlite_output, conn, table, columns):
    #no post processing required for this api call
    print("Processing AIL Demand Data")

    save_dataframe_to_csv(df, path) 
 
    print(f"Data saved to {path}")
    return df
 #---------------------------------------------------    
def final_processing_asset_list_data(api_config, df, output_csv_files, updated_start_date, updated_end_date, original_end_date, year, path, csv_output, sqlite_output, conn, table, columns):
    #no post processing required for this api call
    print("Processing Asset List Data")

    df = convert_columns_to_uppercase(df)

    save_dataframe_to_csv(df, path) 
    
    print(f"Data saved to {path}")
    return df
 #---------------------------------------------------    
def final_processing_generators_above_5MW_data(api_config, df, output_csv_files, updated_start_date, updated_end_date, original_end_date, year, path, csv_output, sqlite_output, conn, table, columns):
    print("Processing Generators Above 5 MW Data")
    #df = pd.DataFrame(df['return']['asset_list'])
    
    print(df.head())

    save_dataframe_to_csv(df, path) 
    
    print(f"Data saved to {path}")
    return df
 #---------------------------------------------------    
def final_processing_historical_spot_price_specific_date_and_range(api_config, df, output_csv_files, updated_start_date, updated_end_date, original_end_date, year, path, csv_output, sqlite_output, conn, table, columns):
    #no post processing required for this api call
    print("Processing Historical Spot Price Specific Date and Range Data")

    print(df.head())

    #save_dataframe_to_csv(df_expanded, path) 
    save_dataframe_to_csv(df, path) 
    
    print(f"Data saved to {path}")
    #return df_expanded
    return df
 #---------------------------------------------------    
def final_processing_historical_spot_price_specific_date(api_config, df, output_csv_files, updated_start_date, updated_end_date, original_end_date, year, path, csv_output, sqlite_output, conn, table, columns):
    #no post processing required for this api call
    print("Processing Historical Spot Price Specific Date Data")

    print(df.head())

    # Extract the "Pool Price Report" data
    #time_series_data = df["return"]["Pool Price Report"]

    # Convert the list of dictionaries to a DataFrame
    #df_expanded = pd.DataFrame(time_series_data)

    #save_dataframe_to_csv(df_expanded, path) 
    save_dataframe_to_csv(df, path) 
    
    print(f"Data saved to {path}")
    #return df_expanded
    return df
 #---------------------------------------------------    
def final_processing_merit_order_data(api_config, df, output_csv_files, updated_start_date, updated_end_date, original_end_date, year, path, csv_output, sqlite_output, conn, table, columns):
    #no post processing required for this api call
    print("Processing Merit Order Data")

    print(f" start_date and end_date data types: {type(updated_start_date)} and {type(original_end_date)}")
    print(f" start_date: {updated_start_date}")
    print(f" end_date: {original_end_date}")
    print(f" df.head(10): {df.head(10)}")
    print(f" df.tail(10): {df.tail(10)}")

    all_data = []

    Counter = 0
    current_date = updated_start_date.date()
    original_end_date = original_end_date.date()

    # current_date = updated_start_date
    # original_end_date = original_end_date

    # Calculate the total hours in the year for the inner progress bar
    total_hours = int((original_end_date - current_date).total_seconds() / 3600) + 1

    #####################################
    #Step 1: Make API for daily data and Loop though each daily report
    #####################################

    # Inner loop for hours within each year
    with tqdm(total=total_hours, desc='Hour', position=1, leave=False) as pbar:
        while current_date <= original_end_date:
            formatted_current_date = current_date.strftime('%Y-%m-%d %H:%M')
            formatted_end_date = original_end_date.strftime('%Y-%m-%d %H:%M')
            print(f" current_date: {formatted_current_date} and end_date: {formatted_end_date}")

            #####################################
            # Step 2: Make API call for daily data
            #####################################

            #this fetches 1 day of merit order data
            print(f" current_date: {current_date}")
            fetched_data = fetch_data(api_config, current_date, original_end_date) 
            # print(f" fetched_data: {fetched_data.head(10)}")
            # print(f" fetched_tail: {fetched_data.tail(10)}")
            print("Appending Data")
            #####################################
            # Step 3: Append Daily data to all_data which will hold all the daily data
            #####################################

            #all_data.append(df)
            all_data.append(fetched_data)
            # print(f" current_date: {current_date}")
            # print(f"Appended df:\n{df.head()}")  # Print what was appended
            # print(f"Appended df:\n{df.tail()}")  # Print what was appended
            # print(f"Length of all_data: {len(all_data)}")  # Check length of all_data

            #####################################
            # Step 4: Update the inner progress bar
            #####################################
            pbar.update(1)

            try:
                print(f"Length of all_data: {len(all_data)}")  # Check length of all_data
            except Exception as e:
                print(f"Error on date {current_date}: {e}")

            current_date += datetime.timedelta(days=1)  # Move to the next day
            # Increment the current date by 1 hour
            #current_date += datetime.timedelta(hours=1)
            Counter = Counter + 1 
            #print(f" Merit Order Daily Counter: {Counter}")

    #######################################
    # Step 5: After the daily loop has completed looping, combine all fetched data into one dataframe
    #######################################
    print("Combining all fetched data into one dateframe")
    df_combined = pd.concat(all_data, ignore_index=True)
    print(f"Appended df.head():\n{df_combined.head()}")  # Print what was appended
    print(f"Appended df.tail():\n{df_combined.tail()}")  # Print what was appended
    
    save_dataframe_to_csv(df_combined, path) 
    print(f"Data saved to {path}")

    return df
 #---------------------------------------------------    
def final_processing_metered_volume_data(api_config, df, output_csv_files, updated_start_date, updated_end_date, original_end_date, year, path, csv_output, sqlite_output, conn, table, columns):

    print("Processing Supply Demand Data")
    print(f" print df with in processing function: {df}")

    Counter = 0
    print(f" Counter: {Counter}")

    all_data = []

    master_asset_ids = set()
    new_asset_ids_per_day = {}

    print(f" start_date and end_date data types: {type(updated_start_date)} and {type(original_end_date)}")
    print(f" start_date: {updated_start_date}")
    print(f" end_date: {original_end_date}")

    # Note the enddate that is passed to this function is based on the

    current_date = updated_start_date.date()
    original_end_date = original_end_date.date()
    #####################################
    #Step 1: Make API for daily data and Loop though each daily report
    #####################################
    
    while current_date <= original_end_date:
        print("Current Date is <= End Date")
        try:
            
            print("Fetching Data")
            #####################################
            # Step 2: Make API call for daily data
            #####################################
            df = fetch_data(api_config, current_date, original_end_date) #>>>>>>>>
            print(f" df: {df}")

            print("Extracting unique Asset_IDs")
            print(df['asset_ID'].unique())
            #####################################
            # Step 3: Extract unique Asset_IDs for the day
            #####################################
            day_asset_ids = set(df['asset_ID'].unique())
            
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
            #####################################
            # Step 3: Append Daily data to all_data which will hold all the daily data
            #####################################
            all_data.append(df)
            print(f"Appended df:\n{df.head()}")  # Print what was appended
            print(f"Appended df:\n{df.tail()}")  # Print what was appended
            print(f"Length of all_data: {len(all_data)}")  # Check length of all_data
            
            try:
                print(f"Length of all_data: {len(all_data)}")  # Check length of all_data
            except Exception as e:
                print(f"Error on date {current_date}: {e}")
                
            current_date += datetime.timedelta(days=1)  # Move to the next day
            
            Counter = Counter + 1 
            print(f" Metered Volumne Daily Counter: {Counter}")
        except Exception as e:
            print(f"Error on date {current_date}: {e}")
        
        print("Processing Metered Volume Data")
        #print(f"Data saved to {path}")
    #######################################
    # Step 5: After the daily loop has completed looping, combine all fetched data into one dataframe
    #######################################
    print("Combining all fetched data into one dateframe")
    df_combined = pd.concat(all_data, ignore_index=True)
    print(f"Appended df:\n{df_combined.head()}")  # Print what was appended
    print(f"Appended df:\n{df_combined.tail()}")  # Print what was appended
    print("Process and save the data as 1 file broken down by Asset_Class")
    
    
    #######################################
    # Step 6: After data has been saved, break down the daily data by the various asset classes
    # that are in the Metered Volumne Report. This will result in files for the following asset classes:
    # DISCO, 
    # EXPORTER, 
    # FWD BUY, 
    # FWD SELL, 
    # GENCO, 
    # GRIDCO, 
    # IMPORTER, 
    # IPP, 
    # LOAD, R
    # ETAILER,
    # SPP
   
    #######################################    

    unique_asset_ids, unique_asset_classes = process_unique_asset_ids(df_combined)
    print(f" unique_asset_ids: {unique_asset_ids}")
    
    #######################################
    # Step 7: Create separate CSV files for each asset_class
    #######################################
    '''
    This separates out the metered volumnes by:

    '''
    #Create file Path
    #Use existing path and take out the .csv file already in it
    reduced_path = remove_filename(path)
    sub_folder =  get_last_folder(reduced_path)
    # Get the path without the sub_folder
    path_without_subfolder = os.path.dirname(reduced_path)


    for asset_class in unique_asset_classes:
        subset_df = df_combined[df_combined['asset_class'] == asset_class]
        
        # Reshape the data for this asset_class
        reshaped_data = reshape_data(subset_df)

        #Create filename and subfolder
        filename = f"{asset_class}.csv"
        new_path = create_path(path_without_subfolder,sub_folder, filename)
        new_path = new_path.replace("\\", "/")  
        #------------------------------------------------------------
        save_dataframe_to_csv(reshaped_data, new_path) #!!!!!!!!!!
        print(f"\nSaved reshaped data for {asset_class} to {new_path}")
        #------------------------------------------------------------
    ################
    print("Process and save the data in separate files by Asset_Class")


    # Add the function call at the end of your routine
    print("Combining metered volume files...")
    #######################################
    # Step 7: Data for generators is stroed in IPP and GENCO files. We need to consolidate these files
    #######################################
    consolidate_generation_files(new_path) #!!!!!!!!!!
    
    #######################################
    # Step 8: Create regional data for the import/export files that are currently only delineated by asset_id
    # This will create a new file with headers:
    # EXPORT_BC,	EXPORT_MT,	EXPORT_SK,	IMPORT_BC,	IMPORT_MT,	IMPORT_SK,
    #######################################
    
    #-----------------
    # Step 8a
    #-----------------
    # Reset current_date as the next function has a similar While loop like the
    # This creates the files: 'aggregated_hourly_import_export_data.csv', 'import_categorized_filtered_sorted.csv' and 'export_categorized_filtered_sorted.csv'
    file_year_suffix = create_regional_import_export_file(path, updated_start_date, original_end_date)

    #-----------------
    # Step 8b
    #-----------------
    # Combines the separate import and export data files 
    # begin_date_utc,begin_date_mpt,IMPORT_BC,IMPORT_MT,IMPORT_SK,EXPORT_BC,EXPORT_MT,EXPORT_SK,TOTAL_IMPORTS,TOTAL_EXPORTS
    # Creates the 'export_import_summary{yyyy}.csv' file
    aggregate_import_exports(file_year_suffix)

    #-----------------
    # Step 8c
    #-----------------
    #This takes the aggregated import export file creates for a given year that looks like this:
    #begin_date_utc,begin_date_mpt,IMPORT_BC,IMPORT_MT,IMPORT_SK,EXPORT_BC,EXPORT_MT,EXPORT_SK,TOTAL_IMPORTS,TOTAL_EXPORTS
    #2024-01-01 07:00,2024-01-01 00:00:00,0.0,34.696,0.0,935.0,0.0,0.0,34.696,935.0

    #And combines it with the associated AIL Demand file for that same year that looks like this:
    #begin_datetime_utc,begin_datetime_mpt,alberta_internal_load,forecast_alberta_internal_load
    #2024-01-01 07:00,2024-01-01 00:00,9809,9779

    #And creates a merged file that looks like this:
    #begin_datetime_utc,begin_datetime_mpt,alberta_internal_load,forecast_alberta_internal_load,IMPORT_BC,IMPORT_MT,IMPORT_SK,EXPORT_BC,EXPORT_MT,EXPORT_SK,TOTAL_IMPORTS,TOTAL_EXPORTS
    #2024-01-01 07:00:00,2024-01-01 00:00:00,9809.0,9779,0.0,34.696,0.0,935.0,0.0,0.0,34.696,935.0
    # Creates the 'combined_Metered_Demand_{year}.csv' file
    combine_demand_with_tie_line_data(file_year_suffix)

    #-----------------
    # Step 8d
    #-----------------
    # Take thge f'combined_Metered_Demand_{specific_year}.csv' file created above
    # and appends it to the combined_Metered_Demand_2000_to_20XX
    append_aggregated_annual_data_with_tie_line_data(file_year_suffix)

    return 
 #---------------------------------------------------    
def final_processing_supply_demand_data(api_config, df, output_csv_files, updated_start_date, updated_end_date, original_end_date, year, path, csv_output, sqlite_output, conn, table, columns):
    #no post processing required for this api call
    save_dataframe_to_csv(df, path) 
    
    print(f"Data saved to {path}")
    return df
 #---------------------------------------------------    
def final_processing_system_marginal_price_data(api_config, df, output_csv_files, updated_start_date, updated_end_date, original_end_date, year, path, csv_output, sqlite_output, conn, table, columns):
    print("Processing System Marginal Price Data")

    save_dataframe_to_csv(df, path) 
    
    print(f"Data saved to {path}")
    return df