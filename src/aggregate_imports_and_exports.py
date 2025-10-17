
import os
import sys

print("Current directory:", os.getcwd())
print("Sys.path:", sys.path)

# Add the project directory to sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.append(project_root)
print("Updated sys.path:", sys.path)

import pandas as pd


########################################
# Create export_import_summary.csv file
########################################


'''
This takes the import and export files that are created in the temp folder for a single year,
and creates an aggregated file with all the individual imports and exports categorized by tie line
and categorized by Import and Export as follows:

IMPORT_BC, IMPORT_MT, IMPORT_SK, EXPORT_BC, EXPORT_MT, EXPORT_SK, TOTAL_IMPORTS, TOTAL_EXPORTS

IMORT FILE
begin_date_utc,begin_date_mpt,ASSET_ID,TOTAL_EXPORTS,ASSET_NAME,ASSET_TYPE,OPERATING_STATUS,POOL_PARTICIPANT_NAME,POOL_PARTICIPANT_ID,NET_TO_GRID_ASSET_FLAG,ASSET_INCL_STORAGE_FLAG,REGION
2024-01-01 07:00,2024-01-01 00:00:00,MOMT,34.696,MOMT MT IMPORT,SOURCE,Active,Morgan Stanley Capital Group Inc.,MSCG,,,IMPORT_MT

EXPORT FILE
begin_date_mpt,ASSET_ID,TOTAL_EXPORTS,ASSET_NAME,ASSET_TYPE,OPERATING_STATUS,POOL_PARTICIPANT_NAME,POOL_PARTICIPANT_ID,NET_TO_GRID_ASSET_FLAG,ASSET_INCL_STORAGE_FLAG,REGION
2024-01-01 07:00,2024-01-01 00:00:00,PW20,935.0,PW20 PWX EXPORT TO BCH,SINK,Active,Powerex Corp.,PWX,,,EXPORT_BC

AGGREGATED FILE
begin_date_utc,begin_date_mpt,IMPORT_BC,IMPORT_MT,IMPORT_SK,EXPORT_BC,EXPORT_MT,EXPORT_SK,TOTAL_IMPORTS,TOTAL_EXPORTS
2024-01-01 07:00,2024-01-01 00:00:00,0.0,0.0,25.0,0.0,0.0,0.0,25.0,0.0

And it aggregates for that given year and creates a file called "aggregated_hourly_import_export_data.csv"

'''

def aggregate_import_exports(file_year_suffix):
    from src.utilities import create_path, save_dataframe_to_csv
    # Load file directory and path
    # file_year_suffix = 2025

    # Load file directory and path
    file_path = r'C:\Users\kaczanor\OneDrive - Enbridge Inc\Documents\Python\Revised-AESO-API-master\output'
    filename1 = f'export_categorized_filtered_sorted_{file_year_suffix}.csv'
    filename2 = f'import_categorized_filtered_sorted_{file_year_suffix}.csv'
    subfolder = "temp"
    export_file_full_path1 = create_path(file_path, subfolder,filename1)
    import_file_full_path2 = create_path(file_path, subfolder,filename2)

    # Load the export data
    #exports_df = pd.read_csv(r'C:\Users\kaczanor\OneDrive - Enbridge Inc\Documents\Python\Revised-AESO-API-master\output\temp\export_categorized_filtered_sorted.csv')
    exports_df = pd.read_csv(export_file_full_path1)


    # Load the import data
    #imports_df = pd.read_csv(r'C:\Users\kaczanor\OneDrive - Enbridge Inc\Documents\Python\Revised-AESO-API-master\output\temp\import_categorized_filtered_sorted.csv')
    imports_df = pd.read_csv(import_file_full_path2)

    # Pivot the export data
    exports_pivot = exports_df.pivot_table(index=['begin_date_utc', 'begin_date_mpt'], 
                                        columns='REGION', 
                                        values='TOTAL_EXPORTS', 
                                        aggfunc='sum', 
                                        fill_value=0).reset_index()

    # Pivot the import data
    imports_pivot = imports_df.pivot_table(index=['begin_date_utc', 'begin_date_mpt'], 
                                        columns='REGION', 
                                        values='TOTAL_IMPORTS', 
                                        aggfunc='sum', 
                                        fill_value=0).reset_index()

    # Merge the pivot tables on 'begin_date_utc' and 'begin_date_mpt' using an outer join
    summary_df = pd.merge(imports_pivot, exports_pivot, on=['begin_date_utc', 'begin_date_mpt'], how='outer', suffixes=('_IMPORT', '_EXPORT'))

    # Fill NaN values with 0 (since a missing value indicates no import/export for that date)
    summary_df.fillna(0, inplace=True)

    # Calculate the total imports and exports
    summary_df['TOTAL_IMPORTS'] = summary_df.filter(like='IMPORT').sum(axis=1)
    summary_df['TOTAL_EXPORTS'] = summary_df.filter(like='EXPORT').sum(axis=1)

    # Reordering columns for the final output
    cols = ['begin_date_utc', 'begin_date_mpt'] + [col for col in summary_df.columns if col not in ['begin_date_utc', 'begin_date_mpt', 'TOTAL_IMPORTS', 'TOTAL_EXPORTS']] + ['TOTAL_IMPORTS', 'TOTAL_EXPORTS']
    summary_df = summary_df[cols]




    # Load file directory and path
    # Write the aggregated data to a summary CSV file
    path = r'C:\Users\kaczanor\OneDrive - Enbridge Inc\Documents\Python\Revised-AESO-API-master\output'
    sub_folder =  "temp"
    filename = f'export_import_summary_{file_year_suffix}.csv'
    #new_path = create_path(path,sub_folder, 'export_import_summary.csv')
    new_path = create_path(path,sub_folder, filename)
    new_path = new_path.replace("\\", "/") 
    print(f"new_path: {new_path}")
    save_dataframe_to_csv(summary_df, new_path) 

    print("Summary file created successfully.")