
import pandas as pd
import os
from tqdm import tqdm

##############################################
#Step 1: Create individual combined demand and import/export file
##############################################
def combine_demand_with_tie_line_data(file_year_suffix):
    from src.utilities import create_path, save_dataframe_to_csv
    '''
    This takes the aggregated import export file creates for a given year that looks like this:

    begin_date_utc,begin_date_mpt,IMPORT_BC,IMPORT_MT,IMPORT_SK,EXPORT_BC,EXPORT_MT,EXPORT_SK,TOTAL_IMPORTS,TOTAL_EXPORTS
    2024-01-01 07:00,2024-01-01 00:00:00,0.0,34.696,0.0,935.0,0.0,0.0,34.696,935.0

    And combines it with the associated AIL Demand file for that same year that looks like this:
    begin_datetime_utc,begin_datetime_mpt,alberta_internal_load,forecast_alberta_internal_load
    2024-01-01 07:00,2024-01-01 00:00,9809,9779

    And creates a merged file that looks like this:

    begin_datetime_utc,begin_datetime_mpt,alberta_internal_load,forecast_alberta_internal_load,IMPORT_BC,IMPORT_MT,IMPORT_SK,EXPORT_BC,EXPORT_MT,EXPORT_SK,TOTAL_IMPORTS,TOTAL_EXPORTS
    2024-01-01 07:00:00,2024-01-01 00:00:00,9809.0,9779,0.0,34.696,0.0,935.0,0.0,0.0,34.696,935.0

    '''

    # Define path to the  "Metered_Demand_YYYY.csv" file
    # Path to the directory containing ail_demand files
    directory = 'C:/Users/kaczanor/OneDrive - Enbridge Inc/Documents/Python/Revised-AESO-API-master/output/Historical AIL Demand'

    # Load the "export_import_summary.csv" file
    source_file_location = r'C:\Users\kaczanor\OneDrive - Enbridge Inc\Documents\Python\Revised-AESO-API-master\output\temp'
    filename = f'export_import_summary_{file_year_suffix}.csv'
    export_import_summary_filepath = os.path.join(source_file_location, filename)

    

    #export_import_by_tielines_summary = pd.read_csv(r'C:\Users\kaczanor\OneDrive - Enbridge Inc\Documents\Python\Revised-AESO-API-master\output\temp\export_import_summary.csv', parse_dates=['begin_date_utc', 'begin_date_mpt'])
    export_import_by_tielines_summary = pd.read_csv(export_import_summary_filepath, parse_dates=['begin_date_utc', 'begin_date_mpt'])
    print(f"export_import_by_tielines_summary.columns: {export_import_by_tielines_summary.columns}")
    print(f"Number of rows in export_import_by_tielines_summary: {len(export_import_by_tielines_summary)}")

    # Optional: specify the year to filter
    # specific_year = input("Enter a specific year to process (or leave blank to process all years): ").strip()
    # specific_year = int(specific_year) if specific_year else None

    specific_year = file_year_suffix

    # Step 3: Disaggregate export_import_by_tielines_summary by year and merge with corresponding ail_demand files
    # Loop through the individual annual Metered_Demand_yyyy files
    #year_list = []

    for filename in tqdm(os.listdir(directory)):
        if filename.startswith('Metered_Demand') and filename.endswith('.csv'):
            try:
                # Extract the year from the filename
                year = int(filename.split('_')[2].split('.')[0])
                if specific_year and year != specific_year:
                    continue
                #year_list.append(year)
            except ValueError:
                print(f"Skipping file {filename} as it does not contain a valid year")
                continue
            
            # Construct the full path to the file
            file_path = os.path.join(directory, filename)
            
            # Load annual ail_demand
            ail_demand= pd.read_csv(file_path, parse_dates=['begin_datetime_utc', 'begin_datetime_mpt'])
            print(f"ail_demand.columns: {ail_demand.columns}")
            print(f"Number of rows in ail_demand: {len(ail_demand)}")
            
            # Filter export_import_by_tielines_summary for the corresponding year
            export_import_by_tielines_summary_filtered = export_import_by_tielines_summary[export_import_by_tielines_summary['begin_date_mpt'].dt.year == year].copy()
            print(f"Number of rows in export_import_by_tielines_summary_filtered before dropping columns: {len(export_import_by_tielines_summary_filtered)}")
            
            # Drop the columns in export_import_by_tielines_summary_filtered that are not needed
            export_import_by_tielines_summary_filtered = export_import_by_tielines_summary_filtered.drop(columns=['begin_date_utc'])
            print(f"Number of rows in export_import_by_tielines_summary_filtered after dropping columns: {len(export_import_by_tielines_summary_filtered)}")
            
            # Merge the dataframes on 'begin_datetime_mpt' and 'begin_date_mpt', performing a left join
            df_combined = pd.merge(ail_demand, export_import_by_tielines_summary_filtered, left_on='begin_datetime_mpt', right_on='begin_date_mpt', how='left')
            
            # Fill NaN values with 0 (since a missing value indicates no import/export for that date)
            df_combined.fillna(0, inplace=True)
            
            # Drop the redundant 'begin_date_mpt' column after merge
            df_combined.drop(columns=['begin_date_mpt'], inplace=True)
            
            # Save the combined data frame which no looks like this:
            # begin_datetime_utc,begin_datetime_mpt,alberta_internal_load,forecast_alberta_internal_load,IMPORT_BC,IMPORT_MT,IMPORT_SK,EXPORT_BC,EXPORT_MT,EXPORT_SK,TOTAL_IMPORTS,TOTAL_EXPORTS
            # 2024-01-01 07:00:00,2024-01-01 00:00:00,9809.0,9779,0.0,34.696,0.0,935.0,0.0,0.0,34.696,935.0
            combined_filename = f'combined_Metered_Demand_{year}.csv'
            combined_filepath = os.path.join(directory, combined_filename)
            df_combined.to_csv(combined_filepath, index=False)
            print(f'Combined data for {year} saved to {combined_filepath}')

            return file_year_suffix

        

##############################################
#Step 2: Create aggregated combined demand and import/export file for all years
##############################################
def append_aggregated_annual_data_with_tie_line_data(file_year_suffix):
    from src.utilities import create_path, save_dataframe_to_csv

    """
    This appends the aggregated annual file with newly created annual file
    """

    # Path to the directory containing the combined files
    combined_directory = 'C:/Users/kaczanor/OneDrive - Enbridge Inc/Documents/Python/Revised-AESO-API-master/output/Historical AIL Demand'

    # Ask user for a specific year to process
    #specific_year = input("Enter a specific year to process (or leave blank to process all years): ").strip()
    specific_year = file_year_suffix

    # Function to check if a year is complete in the dataframe
    def is_full_year(df, year):
        # Calculate expected number of hourly entries for a full year
        expected_hours = 8760
        if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0):  # Leap year check
            expected_hours = 8784
        # Filter dataframe for the specified year
        year_df = df[df['begin_datetime_mpt'].dt.year == year]
        print(len(year_df))
        return len(year_df) == expected_hours

    # Load existing consolidated file if present
    consolidated_file_found = False
    for filename in os.listdir(combined_directory):
        if filename.startswith('combined_Metered_Demand') and '_to_' in filename and filename.endswith('.csv'):
            consolidated_file_found = True
            # Correctly extract the year range from the filename
            parts = filename.split('_')
            min_year = int(parts[3])  # Assuming this is the correct index for the start year
            max_year = int(parts[5].split('.')[0])  # Assuming this is the correct index for the end year
            combined_filepath = os.path.join(combined_directory, filename)
            combined_df = pd.read_csv(combined_filepath, parse_dates=['begin_datetime_utc', 'begin_datetime_mpt'])
            print(f"Loaded consolidated file: {filename}")
            break

    # If no consolidated file found, proceed with original aggregation
    if not consolidated_file_found:
        df_list = []
        year_list = []

        for filename in tqdm(os.listdir(combined_directory)):
            if filename.startswith('combined_Metered_Demand') and filename.endswith('.csv'):
                year = int(filename.split('_')[3].split('.')[0])
                year_list.append(year)
                file_path = os.path.join(combined_directory, filename)
                df = pd.read_csv(file_path, parse_dates=['begin_datetime_mpt'])
                df_list.append(df)
                print(f"Loaded {filename} with {len(df)} rows.")

        if year_list:
            max_year = max(year_list)
            min_year = min(year_list)
            print(f"Max Year: {max_year}")
            print(f"Min Year: {min_year}")

        combined_df = pd.concat(df_list, ignore_index=True)
        print(f"Total rows in the combined dataframe: {len(combined_df)}")

        aggregated_combined_filename = f'combined_Metered_Demand_{min_year}_to_{max_year}.csv'
        combined_filepath = os.path.join(combined_directory, aggregated_combined_filename)
        combined_df.to_csv(combined_filepath, index=False)
        print(f'All combined data saved to {combined_filepath}')

    # If a specific year is provided, attempt to append it
    if specific_year:
        specific_year = int(specific_year)
        if specific_year <= max_year:
            print(f"Data for year {specific_year} is already included in the consolidated file.")
        else:
            # Check if last year in consolidated file is complete
            if not is_full_year(combined_df, max_year):
                print(f"Warning: Data for year {max_year} is incomplete. Please provide full data for year {max_year} as 'combined_Metered_Demand_{max_year}.csv'.")
            else:
                # Load specific year's data
                specific_file = f'combined_Metered_Demand_{specific_year}.csv'
                specific_filepath = os.path.join(combined_directory, specific_file)
                if os.path.exists(specific_filepath):
                    specific_df = pd.read_csv(specific_filepath, parse_dates=['begin_datetime_utc', 'begin_datetime_mpt'])
                    if is_full_year(specific_df, specific_year):
                        combined_df = pd.concat([combined_df, specific_df], ignore_index=True)
                        new_aggregated_filename = f'combined_Metered_Demand_{min_year}_to_{specific_year}.csv'
                        combined_df.to_csv(os.path.join(combined_directory, new_aggregated_filename), index=False)
                        print(f"Appended data for year {specific_year} and saved to {new_aggregated_filename}")
                    else:
                        print(f"Data for year {specific_year} is incomplete. Please provide a full year's data for {specific_year} as 'combined_Metered_Demand_{specific_year}.csv'.")
                else:
                    print(f"File for year {specific_year} not found. Please ensure 'combined_Metered_Demand_{specific_year}.csv' exists in the directory.")


# # Path to the directory containing the combined files
# combined_directory = 'C:/Users/kaczanor/OneDrive - Enbridge Inc/Documents/Python/Revised-AESO-API-master/output/Historical AIL Demand'

# # List to hold dataframes
# df_list = []
# year_list = []

# # Step 1: Iterate through the combined files and read them into dataframes
# for filename in tqdm(os.listdir(combined_directory)):
#     if filename.startswith('combined_Metered_Demand') and filename.endswith('.csv'):
#         year = int(filename.split('_')[3].split('.')[0])
#         year_list.append(year)
#         file_path = os.path.join(combined_directory, filename)
#         df = pd.read_csv(file_path, parse_dates=['begin_datetime_utc', 'begin_datetime_mpt'])
#         df_list.append(df)
#         print(f"Loaded {filename} with {len(df)} rows.")

# # Calculate max and min if the list is not empty
# # Use these to define the year ranges in the consolidated 
# # files in the next step
# if year_list:
#     max_year = max(year_list)
#     min_year = min(year_list)
#     print(f"Max Year: {max_year}")
#     print(f"Min Year: {min_year}")
# else:
#     print("No valid year data found in filenames.")

# # Step 2: Concatenate all dataframes in the list
# combined_df = pd.concat(df_list, ignore_index=True)
# print(f"Total rows in the combined dataframe: {len(combined_df)}")

# # Step 3: Save the combined dataframe to a new CSV file
# aggregated_combined_filename = f'combined_Metered_Demand_{min_year}_to_{min_year}.csv'
# combined_filepath = os.path.join(combined_directory, aggregated_combined_filename)
# combined_df.to_csv(combined_filepath, index=False)
# print(f'All combined data saved to {combined_filepath}')