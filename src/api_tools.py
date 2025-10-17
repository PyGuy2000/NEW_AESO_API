
import os
import sys
import pandas as pd
import datetime
from dotenv import load_dotenv
from src.utilities import print_directory_tree
from src.utilities import print_folder_list
from src.utilities import remove_folder_contents
from src.utilities import create_path
from src.utilities import fetch_data
import requests
from tqdm import tqdm
import io


load_dotenv()

def get_api_credientials(service_name):

    aeso_key = os.getenv(f"{service_name}_PRIMARY_API_KEY") 
    base_url = os.getenv(f"{service_name}_BASE_URL")
    output_folder = os.getenv(f"{service_name}_OUTPUT_FOLDER_PATH")

    return aeso_key, base_url, output_folder

def build_api_request_repository(
                api_activation_dict, 
                aeso_key,base_url, 
                start_date, 
                end_date, 
                explicit_end_date, 
                str_start_date, 
                str_explicit_end_date, 
                year, 
                operating_status, 
                asset_type, 
                output_folder
                ):
    
    api_data_dict = {}


    api_data_dict = {
    # 'OLD_AESO' : {
    #      'Pool_Participant_List' : {
    #         'function_name' : 'final_processing_pool_participant_data',
    #         'data_type' : 'list',
    #         'reporting_limit': None,
    #         'headers' : {"X-API-Key": aeso_key},
    #         'params' : None,
    #         'api_url' : f"{base_url}report/v1/poolparticipantlist",
    #         'return_key' : "Actual Forecast Report",
    #         'sub_folder_template' : 'Pool_Participants/',
    #         'file_name_template' : f'Pool_Participants_{year}.csv',
    #         'output_csv_files' : f"{output_folder}Pool_Participants/Pool_Participants_{year}.csv",
    #         'column_order': [],
    #         'normalize_json' :  False,
    #         'record_path_for_normalize_json' : None,
    #         'json_normalize_keys' : None,
    #         'meta_for_normalized_json' : None,
    #         'normalized_concatenation_keys': None,
    #         'json_explode' : False,
    #         'removed_data_lists' : None,
    #         'special_note' : "Nothing",
    #         'run_option' : api_activation_dict['pool_participant_data_state'],
    #         'consolidate_files' : False,
    #         'output_consolidated_csv_files' : None
    #     },
    #     'Operating Reserve Offer Control Report' : {
    #         'function_name' : 'final_processing_operating_reserve_offer_control_data',
    #         'data_type' : 'list',
    #         'reporting_limit': None,
    #         'headers' : {'accept': 'application/json',"X-API-Key": aeso_key},
    #         'params' : {'startDate' : {start_date}},
    #         'api_url' : f"{base_url}report/v1/operatingReserveOfferControl",
    #         'return_key' : "Operating Reserve Trade Merit Order",
    #         'sub_folder_template' : 'Operating Reserve Offer Control/',
    #         'file_name_template' : f'Operating_Reserves_{year}.csv',
    #         'output_csv_files' : f"{output_folder}Operating Reserve Offer Control/Operating_Reserves_{year}.csv",
    #         'column_order': [],
    #         'normalize_json' :  True,
    #         'record_path_for_normalize_json' : 'operating_reserve_blocks',
    #         'json_normalize_keys' : None,
    #         'meta_for_normalized_json' : ['begin_datetime_utc', 'begin_datetime_mpt'],
    #         'normalized_concatenation_keys': None,
    #         'json_explode' : False,
    #         'removed_data_lists' : None,
    #         'special_note' : "Data are only available from.2012-03-12",
    #         'run_option' : api_activation_dict['operating_reserve_offer_control_data_state'],
    #         'consolidate_files' : False,
    #         'output_consolidated_csv_files' : None
    #     },
    #     'AIL_Demand' : {
    #         'function_name' : 'final_processing_actual_forecast_report_data',
    #         'data_type' : 'time series',
    #         'reporting_limit': None,
    #         'headers' : {"X-API-Key": aeso_key},
    #         'params' : {"startDate": {start_date}, "endDate": {end_date}},
    #         'api_url' : f"{base_url}report/v1/load/albertaInternalLoad",
    #         'return_key' : "Actual Forecast Report",
    #         'sub_folder_template' : 'Historical AIL Demand/',
    #         'file_name_template' : f'Metered_Demand_{year}.csv',
    #         'output_csv_files' : f"{output_folder}Historical AIL Demand/Metered_Demand_{year}.csv",
    #         'column_order': [],
    #         'normalize_json' :  False,
    #         'record_path_for_normalize_json' : None,
    #         'json_normalize_keys' : None,
    #         'meta_for_normalized_json' : None,
    #         'normalized_concatenation_keys': None,
    #         'json_explode' : False,
    #         'removed_data_lists' : None,
    #         'special_note' : "Nothing",
    #         'run_option' : api_activation_dict['actual_forecast_report_data_state'],
    #         'consolidate_files' : False,
    #         'output_consolidated_csv_files' : None
    #     },
    #     'Asset_List' : {
    #         'function_name' : 'final_processing_asset_list_data',
    #         'data_type' : 'list',
    #         'reporting_limit': None,
    #         'headers' : {"X-API-Key": aeso_key},
    #         'params' : {"operating_status": {operating_status}, "asset_type": {asset_type}},
    #         'api_url' : f"{base_url}report/v1/assetlist",
    #         'return_key' : None,
    #         'sub_folder_template' : 'Asset List/',
    #         'file_name_template' : 'Asset_Lists.csv',
    #         'output_csv_files' : f'{output_folder}Asset List/Asset_Lists.csv',
    #         'column_order': [],
    #         'normalize_json' :  False,
    #         "record_path_for_normalize_json" : None,
    #         'json_normalize_keys' : None,
    #         'meta_for_normalized_json' : None,
    #         'normalized_concatenation_keys': None,
    #         'json_explode' : False,
    #         'removed_data_lists' : None,
    #         'special_note' : "Nothing",
    #         'run_option' : api_activation_dict['asset_list_data_state'],
    #         'consolidate_files' : False,
    #         'output_consolidated_csv_files' : None
    #     },
    #     'Generators_Above_5MW' : {
    #         'function_name' : 'final_processing_generators_above_5MW_data',
    #         'data_type' : 'list',
    #         'reporting_limit': None,
    #         'headers' : {"X-API-Key": aeso_key},
    #         'params' : {},
    #         'api_url' : f"{base_url}report/v1/csd/generation/assets/current",
    #         'return_key' : "asset_list",
    #         'sub_folder_template' : 'Generation Asset List/',
    #         'file_name_template' : 'Gen_Assets.csv',
    #         'output_csv_files' : f'{output_folder}Generation Asset List/Gen_Assets.csv',
    #         'column_order': [],
    #         'normalize_json' :  False,
    #         "record_path_for_normalize_json" : None,
    #         'json_normalize_keys' : None,
    #         'meta_for_normalized_json' : None,
    #         'normalized_concatenation_keys': None,
    #         'json_explode' : False,
    #         'removed_data_lists' : None,
    #         'special_note' : "Nothing",
    #         'run_option' : api_activation_dict['generators_above_5MW_data_state'],
    #         'consolidate_files' : False,
    #         'output_consolidated_csv_files' : None
    #     },
    #     'Historical_Pool_Price_Date_And_Range' : {
    #         'function_name' : 'final_processing_historical_spot_price_specific_date_and_range',
    #         'data_type' : 'time series',
    #         'reporting_limit': None,
    #         'headers' : {"X-API-Key": aeso_key},
    #         'params' : {"startDate": {start_date}, "endDate": {explicit_end_date}},
    #         'api_url' : f"{base_url}report/v1.1/price/poolPrice",
    #         'return_key' : "Pool Price Report",
    #         'sub_folder_template' : 'Spot_Prices/',
    #         'file_name_template' : f'pool_price_data_{year}.csv',
    #         'output_csv_files' : f'{output_folder}Spot_Prices/pool_price_data_{year}.csv',
    #         'column_order': [],
    #         'normalize_json' :  False,
    #         "record_path_for_normalize_json" : None,
    #         'json_normalize_keys' : None,
    #         'meta_for_normalized_json' : None,
    #         'normalized_concatenation_keys': None,
    #         'json_explode' : False,
    #         'removed_data_lists' : None,
    #         'special_note' : "This API Call can only produce data for 366 days",
    #         'run_option' : api_activation_dict['historical_spot_price_specific_date_and_range_state'],
    #         'consolidate_files' : True,
    #         'output_consolidated_csv_files' : f'{output_folder}Spot_Prices/merged_pool_price_data_{start_date}_to_{end_date}.csv'
    #     },
    #     'Historical_Pool_Price_Date' : {
    #         'function_name' : 'final_processing_historical_spot_price_specific_date',
    #         'data_type' : 'time series',
    #         'reporting_limit': None,
    #         'headers' : {"X-API-Key": aeso_key},
    #         'params' : {"startDate": {start_date}, "endDate": {explicit_end_date}},
    #         'api_url' : f"{base_url}report/v1.1/price/poolPrice",
    #         'return_key' : "Pool Price Report",
    #         'sub_folder_template' : 'Historical Pool Price/',
    #         'file_name_template' : f'pool_price_data_{year}.csv',
    #         'output_csv_files' : f'{output_folder}Historical Pool Price/pool_price_data_{year}.csv',
    #         'column_order': [],
    #         'normalize_json' :  False,
    #         "record_path_for_normalize_json" : None,
    #         'json_normalize_keys' : None,
    #         'meta_for_normalized_json' : None,
    #         'normalized_concatenation_keys': None,
    #         'json_explode' : False,
    #         'removed_data_lists' : None,
    #         'special_note' : "This report is available for a maximum of 366 days of data",
    #         'run_option' : api_activation_dict['historical_spot_price_specific_date_state'],
    #         'consolidate_files' : False,
    #         'output_consolidated_csv_files' : None
    #     },
    #     'Merit_Order_Data' : {
    #         'function_name' : 'final_processing_merit_order_data',
    #         'data_type' : 'time series',
    #         'reporting_limit': None,
    #         'headers' : {'accept': 'application/json', 'X-API-Key': aeso_key},
    #         'params' : {'startDate': start_date},
    #         'api_url' : f"{base_url}report/v1/meritOrder/energy",
    #         'return_key' : "data",
    #         'sub_folder_template' : 'Merit Order Curves/',
    #         'file_name_template' : f'merit_order_data_{year}.csv',
    #         'output_csv_files' : f'{output_folder}Merit Order Curves/merit_order_data_{year}.csv',
    #         'column_order': [
    #             'begin_dateTime_utc', 'begin_dateTime_mpt', 'import_or_export', 'asset_ID', 'block_number',
    #             'block_price', 'from_MW', 'to_MW', 'block_size', 'available_MW', 'dispatched?', 
    #             'dispatched_MW', 'flexible?', 'offer_control'],
    #         'normalize_json' :  True,
    #         'record_path_for_normalize_json' : 'energy_blocks',
    #         'json_normalize_keys' : ['data', 'return'],
    #         'meta_for_normalized_json' : ['begin_dateTime_utc', 'begin_dateTime_mpt'],
    #         'normalized_concatenation_keys': None,
    #         'json_explode' : False,
    #         'removed_data_lists' : None,
    #         'special_note' : "The EMMO snapshot data is available 60 days after the date of the snapshot, first available from September 1, 2009. The data from 1-Sep-2009 to 1-Sep-2014 is the Merit Order at the 30th min. of the settlement interval.The data after 1-Sep-2014 is the last Merit Order of the settlement interval.",
    #         'run_option' : api_activation_dict['merit_order_data_state'],
    #         'consolidate_files' : False,
    #         'output_consolidated_csv_files' : None 
    #     },
    #     'Metered_Volume_Data' : {
    #         'function_name' : 'final_processing_metered_volume_data',
    #         'data_type' : 'time series',
    #         'reporting_limit': None,
    #         'headers' : {"X-API-Key": aeso_key},
    #         'params' : {'startDate': start_date},
    #         'api_url' : f"{base_url}report/v1/meteredvolume/details",
    #         'return_key' : None,
    #         'sub_folder_template' : 'Metered Volumes/',
    #         'file_name_template' : f'metered_volumes_{year}.csv',
    #         'output_csv_files' : f'{output_folder}Metered Volumes/metered_volumes_{year}.csv',
    #         'column_order': [],
    #         'normalize_json' :  True,
    #         "record_path_for_normalize_json" : 'asset_list',
    #         'json_normalize_keys' : ['return', 'asset_list'],
    #         'meta_for_normalized_json' : 'metered_volume_list',
    #         'normalized_concatenation_keys': ['asset_ID', 'asset_class'],
    #         'json_explode' : True,
    #         'removed_data_lists' : None,
    #         'special_note' : "Nothing",
    #         'run_option' : api_activation_dict['metered_volume_data_state'],
    #         'consolidate_files' : False,
    #         'output_consolidated_csv_files' : None
    #     },
    #     'Supply_Demand_Data_Generation' : {
    #         'function_name' : 'final_processing_supply_demand_data',
    #         'data_type' : 'list',
    #         'reporting_limit': None,
    #         'headers' : {'X-API-Key': aeso_key},
    #         'params' : {'startDate': {None},'endDate': {None}},
    #         'api_url' : f"{base_url}report/v1/csd/summary/current",
    #         'return_key' : None,
    #         'sub_folder_template' : 'Supply and Demand/',
    #         'file_name_template' : f'CSD_data_generation_{year}.csv',
    #         'output_csv_files' : f'{output_folder}Supply and Demand/CSD_data_generation_{year}.csv',
    #         'column_order': [],
    #         'normalize_json' :  True,
    #         "record_path_for_normalize_json" : ['return', 'generation_data_list'],
    #         'json_normalize_keys' : None,
    #         'meta_for_normalized_json' : None,
    #         'normalized_concatenation_keys': None,
    #         'json_explode' : False,
    #         'removed_data_lists' : None,
    #         'special_note' : "Nothing",
    #         'run_option' : api_activation_dict['supply_demand_data_generation_state'],
    #         'consolidate_files' : False,
    #         'output_consolidated_csv_files' : None 
    #     },
    #       'Supply_Demand_Data_Interties' : {
    #         'function_name' : 'final_processing_supply_demand_data',
    #         'data_type' : 'list',
    #         'reporting_limit': None,
    #         'headers' : {'X-API-Key': aeso_key},
    #         'params' : {'startDate': {None},'endDate': {None}},
    #         'api_url' : f"{base_url}report/v1/csd/summary/current",
    #         'return_key' : None,
    #         'sub_folder_template' : 'Supply and Demand/',
    #         'file_name_template' : f'CSD_data_interties_{year}.csv',
    #         'output_csv_files' : f'{output_folder}Supply and Demand/CSD_data_interties_{year}.csv',
    #         'column_order': [],
    #         'normalize_json' :  True,
    #         "record_path_for_normalize_json" : ['return', 'interchange_list'],
    #         'json_normalize_keys' : None,
    #         'meta_for_normalized_json' : None,
    #         'normalized_concatenation_keys': None,
    #         'json_explode' : False,
    #         'removed_data_lists' : None,
    #         'special_note' : "Nothing",
    #         'run_option' : api_activation_dict['supply_demand_data_intertie_state'],
    #         'consolidate_files' : False,
    #         'output_consolidated_csv_files' : None 
    #     },
    #        'Supply_Demand_Data_Summary' : {
    #         'function_name' : 'final_processing_supply_demand_data',
    #         'data_type' : 'list',
    #         'reporting_limit': None,
    #         'headers' : {'X-API-Key': aeso_key},
    #         'params' : {'startDate': {None},'endDate': {None}},
    #         'api_url' : f"{base_url}report/v1/csd/summary/current",
    #         'return_key' : None,
    #         'sub_folder_template' : 'Supply and Demand/',
    #         'file_name_template' : f'CSD_data_summary_{year}.csv',
    #         'output_csv_files' : f'{output_folder}Supply and Demand/CSD_data_summary_{year}.csv',
    #         'column_order': [],
    #         'normalize_json' :  False,
    #         "record_path_for_normalize_json" : None,
    #         'json_normalize_keys' : None,
    #         'meta_for_normalized_json' : None,
    #         'normalized_concatenation_keys': None,
    #         'json_explode' : False,
    #         'removed_data_lists' : ['generation_data_list', 'interchange_list'],
    #         'special_note' : "Nothing",
    #         'run_option' : api_activation_dict['supply_demand_data_summary_state'],
    #         'consolidate_files' : False,
    #         'output_consolidated_csv_files' : None 
    #     },
    #     'System_Marginal_Price_Data' : {
    #         'function_name' : 'final_processing_system_marginal_price_data',
    #         'data_type' : 'time series',
    #         'reporting_limit': 182,
    #         'headers' : {"X-API-Key": aeso_key},
    #         'params' : {"startDate": {start_date}, "endDate": {explicit_end_date}},
    #         'api_url' : f"{base_url}report/v1.1/price/systemMarginalPrice",
    #         'return_key' : "System Marginal Price Report",
    #         'sub_folder_template' : 'System_Marginal_Price/',
    #         'file_name_template' : f'System_Marginal_Price_{str_start_date}_to_{str_explicit_end_date}.csv',
    #         'output_csv_files' : f'{output_folder}System_Marginal_Price_{str_start_date}_to_{str_explicit_end_date}.csv',
    #         'column_order': [],
    #         'normalize_json' :  False,
    #         "record_path_for_normalize_json" : None,
    #         'json_normalize_keys' : None,
    #         'meta_for_normalized_json' : None,
    #         'normalized_concatenation_keys': None,
    #         'json_explode' : False,
    #         'removed_data_lists' : None,
    #         'special_note' : "Nothing",
    #         'run_option' : api_activation_dict['system_marginal_price_data_state'],
    #         'consolidate_files' : False,
    #         'output_consolidated_csv_files' : None
    # }
    # },
     'NEW_AESO' : {
         'Pool_Participant_List' : {
            'function_name' : 'final_processing_pool_participant_data',
            'data_type' : 'list',
            'reporting_limit': None,
            'headers' : {"API-KEY": aeso_key},
            'params' : None,
            'api_url' : f"{base_url}report/v1/poolparticipantlist",
            'return_key' : "Actual Forecast Report",
            'sub_folder_template' : 'Pool_Participants/',
            'file_name_template' : f'Pool_Participants_{year}.csv',
            'output_csv_files' : f"{output_folder}Pool_Participants/Pool_Participants_{year}.csv",
            'column_order': [],
            'normalize_json' :  False,
            'record_path_for_normalize_json' : None,
            'json_normalize_keys' : None,
            'meta_for_normalized_json' : None,
            'normalized_concatenation_keys': None,
            'json_explode' : False,
            'removed_data_lists' : None,
            'special_note' : "Nothing",
            'run_option' : api_activation_dict['pool_participant_data_state'],
            'consolidate_files' : False,
            'output_consolidated_csv_files' : None
        },
        'Operating Reserve Offer Control Report' : {
            'function_name' : 'final_processing_operating_reserve_offer_control_data',
            'data_type' : 'list',
            'reporting_limit': None,
            'headers' : {'accept': 'application/json',"API-KEY": aeso_key},
            'params' : {'startDate' : {start_date}},
            'api_url' : f"{base_url}report/v1/operatingReserveOfferControl",
            'return_key' : "Operating Reserve Trade Merit Order",
            'sub_folder_template' : 'Operating Reserve Offer Control/',
            'file_name_template' : f'Operating_Reserves_{year}.csv',
            'output_csv_files' : f"{output_folder}Operating Reserve Offer Control/Operating_Reserves_{year}.csv",
            'column_order': [],
            'normalize_json' :  True,
            'record_path_for_normalize_json' : 'operating_reserve_blocks',
            'json_normalize_keys' : None,
            'meta_for_normalized_json' : ['begin_datetime_utc', 'begin_datetime_mpt'],
            'normalized_concatenation_keys': None,
            'json_explode' : False,
            'removed_data_lists' : None,
            'special_note' : "Data are only available from.2012-03-12",
            'run_option' : api_activation_dict['operating_reserve_offer_control_data_state'],
            'consolidate_files' : False,
            'output_consolidated_csv_files' : None
        },
        'AIL_Demand' : {
            'function_name' : 'final_processing_actual_forecast_report_data',
            'data_type' : 'time series',
            'reporting_limit': None,
            'headers' : {'Cache-Control': 'no-cache',"API-KEY": aeso_key},
            'params' : {"startDate": {start_date}, "endDate": {end_date}},
            'api_url' : f"{base_url}actualforecast-api/v1/load/albertaInternalLoad",
            'return_key' : "Actual Forecast Report",
            'sub_folder_template' : 'Historical AIL Demand/',
            'file_name_template' : f'Metered_Demand_{year}.csv',
            'output_csv_files' : f"{output_folder}Historical AIL Demand/Metered_Demand_{year}.csv",
            'column_order': [],
            'normalize_json' :  False,
            'record_path_for_normalize_json' : None,
            'json_normalize_keys' : None,
            'meta_for_normalized_json' : None,
            'normalized_concatenation_keys': None,
            'json_explode' : False,
            'removed_data_lists' : None,
            'special_note' : "Nothing",
            'run_option' : api_activation_dict['actual_forecast_report_data_state'],
            'consolidate_files' : False,
            'output_consolidated_csv_files' : None
        },
        'Asset_List' : {
            'function_name' : 'final_processing_asset_list_data',
            'data_type' : 'list',
            'reporting_limit': None,
            'headers' : {"API-KEY": aeso_key},
            'params' : {
                #"asset_ID" : "ALL",
                #"pool_participant_ID": "ALL",
                "operating_status": "ALL", 
                "asset_type": "ALL",
                },   
            'api_url' : f"{base_url}assetlist-api/v1/assetlist",
            'return_key' : None,
            'sub_folder_template' : 'Asset List/',
            'file_name_template' : 'Asset_Lists.csv',
            'output_csv_files' : f'{output_folder}Asset List/Asset_Lists.csv',
            'column_order': [],
            'normalize_json' :  False,
            "record_path_for_normalize_json" : None,
            'json_normalize_keys' : None,
            'meta_for_normalized_json' : None,
            'normalized_concatenation_keys': None,
            'json_explode' : False,
            'removed_data_lists' : None,
            'special_note' : "Nothing",
            'run_option' : api_activation_dict['asset_list_data_state'],
            'consolidate_files' : False,
            'output_consolidated_csv_files' : None
        },
        'Generators_Above_5MW' : {
            'function_name' : 'final_processing_generators_above_5MW_data',
            'data_type' : 'list',
            'reporting_limit': None,
            'headers' : {"API-KEY": aeso_key},
            'params' : {},
            'api_url' : f"{base_url}report/v1/csd/generation/assets/current",
            'return_key' : "asset_list",
            'sub_folder_template' : 'Generation Asset List/',
            'file_name_template' : 'Gen_Assets.csv',
            'output_csv_files' : f'{output_folder}Generation Asset List/Gen_Assets.csv',
            'column_order': [],
            'normalize_json' :  False,
            "record_path_for_normalize_json" : None,
            'json_normalize_keys' : None,
            'meta_for_normalized_json' : None,
            'normalized_concatenation_keys': None,
            'json_explode' : False,
            'removed_data_lists' : None,
            'special_note' : "Nothing",
            'run_option' : api_activation_dict['generators_above_5MW_data_state'],
            'consolidate_files' : False,
            'output_consolidated_csv_files' : None
        },
        'Historical_Pool_Price_Date_And_Range' : {
            'function_name' : 'final_processing_historical_spot_price_specific_date_and_range',
            'data_type' : 'time series',
            'reporting_limit': None,
            'headers' : {"API-KEY": aeso_key},
            'params' : {"startDate": {start_date}, "endDate": {explicit_end_date}},
            'api_url' : f"{base_url}report/v1.1/price/poolPrice",
            'return_key' : "Pool Price Report",
            'sub_folder_template' : 'Spot_Prices/',
            'file_name_template' : f'pool_price_data_{year}.csv',
            'output_csv_files' : f'{output_folder}Spot_Prices/pool_price_data_{year}.csv',
            'column_order': [],
            'normalize_json' :  False,
            "record_path_for_normalize_json" : None,
            'json_normalize_keys' : None,
            'meta_for_normalized_json' : None,
            'normalized_concatenation_keys': None,
            'json_explode' : False,
            'removed_data_lists' : None,
            'special_note' : "This API Call can only produce data for 366 days",
            'run_option' : api_activation_dict['historical_spot_price_specific_date_and_range_state'],
            'consolidate_files' : True,
            'output_consolidated_csv_files' : f'{output_folder}Spot_Prices/merged_pool_price_data_{start_date}_to_{end_date}.csv'
        },
        'Historical_Pool_Price_Date' : {
            'function_name' : 'final_processing_historical_spot_price_specific_date',
            'data_type' : 'time series',
            'reporting_limit': None,
            'headers' : {"API-KEY": aeso_key},
            'params' : {"startDate": {start_date}, "endDate": {explicit_end_date}},
            'api_url' : f"{base_url}report/v1.1/price/poolPrice",
            'return_key' : "Pool Price Report",
            'sub_folder_template' : 'Historical Pool Price/',
            'file_name_template' : f'pool_price_data_{year}.csv',
            'output_csv_files' : f'{output_folder}Historical Pool Price/pool_price_data_{year}.csv',
            'column_order': [],
            'normalize_json' :  False,
            "record_path_for_normalize_json" : None,
            'json_normalize_keys' : None,
            'meta_for_normalized_json' : None,
            'normalized_concatenation_keys': None,
            'json_explode' : False,
            'removed_data_lists' : None,
            'special_note' : "This report is available for a maximum of 366 days of data",
            'run_option' : api_activation_dict['historical_spot_price_specific_date_state'],
            'consolidate_files' : False,
            'output_consolidated_csv_files' : None
        },
        'Merit_Order_Data' : {
            'function_name' : 'final_processing_merit_order_data',
            'data_type' : 'time series',
            'reporting_limit': None,
            'headers' : {'accept': 'application/json', 'API-KEY': aeso_key},
            'params' : {'startDate': start_date},
            'api_url' : f"{base_url}report/v1/meritOrder/energy",
            'return_key' : "data",
            'sub_folder_template' : 'Merit Order Curves/',
            'file_name_template' : f'merit_order_data_{year}.csv',
            'output_csv_files' : f'{output_folder}Merit Order Curves/merit_order_data_{year}.csv',
            'column_order': [
                'begin_dateTime_utc', 'begin_dateTime_mpt', 'import_or_export', 'asset_ID', 'block_number',
                'block_price', 'from_MW', 'to_MW', 'block_size', 'available_MW', 'dispatched?', 
                'dispatched_MW', 'flexible?', 'offer_control'],
            'normalize_json' :  True,
            'record_path_for_normalize_json' : 'energy_blocks',
            'json_normalize_keys' : ['data', 'return'],
            'meta_for_normalized_json' : ['begin_dateTime_utc', 'begin_dateTime_mpt'],
            'normalized_concatenation_keys': None,
            'json_explode' : False,
            'removed_data_lists' : None,
            'special_note' : "The EMMO snapshot data is available 60 days after the date of the snapshot, first available from September 1, 2009. The data from 1-Sep-2009 to 1-Sep-2014 is the Merit Order at the 30th min. of the settlement interval.The data after 1-Sep-2014 is the last Merit Order of the settlement interval.",
            'run_option' : api_activation_dict['merit_order_data_state'],
            'consolidate_files' : False,
            'output_consolidated_csv_files' : None 
        },
        'Metered_Volume_Data' : {
            'function_name' : 'final_processing_metered_volume_data',
            'data_type' : 'time series',
            'reporting_limit': None,
            'headers' : {'Cache-Control': 'no-cache',"API-KEY": aeso_key},
            'params' : {'startDate': start_date},
            'api_url' : f"{base_url}meteredvolume-api/v1/meteredvolume/details",
            'return_key' : None,
            'sub_folder_template' : 'Metered Volumes/',
            'file_name_template' : f'metered_volumes_{year}.csv',
            'output_csv_files' : f'{output_folder}Metered Volumes/metered_volumes_{year}.csv',
            'column_order': [],
            'normalize_json' :  True,
            "record_path_for_normalize_json" : 'asset_list',
            'json_normalize_keys' : ['return', 'asset_list'],
            'meta_for_normalized_json' : 'metered_volume_list',
            'normalized_concatenation_keys': ['asset_ID', 'asset_class'],
            'json_explode' : True,
            'removed_data_lists' : None,
            'special_note' : "Nothing",
            'run_option' : api_activation_dict['metered_volume_data_state'],
            'consolidate_files' : False,
            'output_consolidated_csv_files' : None
        },
        'Supply_Demand_Data_Generation' : {
            'function_name' : 'final_processing_supply_demand_data',
            'data_type' : 'list',
            'reporting_limit': None,
            'headers' : {'API-KEY': aeso_key},
            'params' : {'startDate': {None},'endDate': {None}},
            'api_url' : f"{base_url}report/v1/csd/summary/current",
            'return_key' : None,
            'sub_folder_template' : 'Supply and Demand/',
            'file_name_template' : f'CSD_data_generation_{year}.csv',
            'output_csv_files' : f'{output_folder}Supply and Demand/CSD_data_generation_{year}.csv',
            'column_order': [],
            'normalize_json' :  True,
            "record_path_for_normalize_json" : ['return', 'generation_data_list'],
            'json_normalize_keys' : None,
            'meta_for_normalized_json' : None,
            'normalized_concatenation_keys': None,
            'json_explode' : False,
            'removed_data_lists' : None,
            'special_note' : "Nothing",
            'run_option' : api_activation_dict['supply_demand_data_generation_state'],
            'consolidate_files' : False,
            'output_consolidated_csv_files' : None 
        },
          'Supply_Demand_Data_Interties' : {
            'function_name' : 'final_processing_supply_demand_data',
            'data_type' : 'list',
            'reporting_limit': None,
            'headers' : {'API-KEY': aeso_key},
            'params' : {'startDate': {None},'endDate': {None}},
            'api_url' : f"{base_url}report/v1/csd/summary/current",
            'return_key' : None,
            'sub_folder_template' : 'Supply and Demand/',
            'file_name_template' : f'CSD_data_interties_{year}.csv',
            'output_csv_files' : f'{output_folder}Supply and Demand/CSD_data_interties_{year}.csv',
            'column_order': [],
            'normalize_json' :  True,
            "record_path_for_normalize_json" : ['return', 'interchange_list'],
            'json_normalize_keys' : None,
            'meta_for_normalized_json' : None,
            'normalized_concatenation_keys': None,
            'json_explode' : False,
            'removed_data_lists' : None,
            'special_note' : "Nothing",
            'run_option' : api_activation_dict['supply_demand_data_intertie_state'],
            'consolidate_files' : False,
            'output_consolidated_csv_files' : None 
        },
           'Supply_Demand_Data_Summary' : {
            'function_name' : 'final_processing_supply_demand_data',
            'data_type' : 'list',
            'reporting_limit': None,
            'headers' : {'API-KEY': aeso_key},
            'params' : {'startDate': {None},'endDate': {None}},
            'api_url' : f"{base_url}report/v1/csd/summary/current",
            'return_key' : None,
            'sub_folder_template' : 'Supply and Demand/',
            'file_name_template' : f'CSD_data_summary_{year}.csv',
            'output_csv_files' : f'{output_folder}Supply and Demand/CSD_data_summary_{year}.csv',
            'column_order': [],
            'normalize_json' :  False,
            "record_path_for_normalize_json" : None,
            'json_normalize_keys' : None,
            'meta_for_normalized_json' : None,
            'normalized_concatenation_keys': None,
            'json_explode' : False,
            'removed_data_lists' : ['generation_data_list', 'interchange_list'],
            'special_note' : "Nothing",
            'run_option' : api_activation_dict['supply_demand_data_summary_state'],
            'consolidate_files' : False,
            'output_consolidated_csv_files' : None 
        },
        'System_Marginal_Price_Data' : {
            'function_name' : 'final_processing_system_marginal_price_data',
            'data_type' : 'time series',
            'reporting_limit': 182,
            'headers' : {"API-KEY": aeso_key},
            'params' : {"startDate": {start_date}, "endDate": {explicit_end_date}},
            'api_url' : f"{base_url}report/v1.1/price/systemMarginalPrice",
            'return_key' : "System Marginal Price Report",
            'sub_folder_template' : 'System_Marginal_Price/',
            'file_name_template' : f'System_Marginal_Price_{str_start_date}_to_{str_explicit_end_date}.csv',
            'output_csv_files' : f'{output_folder}System_Marginal_Price_{str_start_date}_to_{str_explicit_end_date}.csv',
            'column_order': [],
            'normalize_json' :  False,
            "record_path_for_normalize_json" : None,
            'json_normalize_keys' : None,
            'meta_for_normalized_json' : None,
            'normalized_concatenation_keys': None,
            'json_explode' : False,
            'removed_data_lists' : None,
            'special_note' : "Nothing",
            'run_option' : api_activation_dict['system_marginal_price_data_state'],
            'consolidate_files' : False,
            'output_consolidated_csv_files' : None
            }
        },
    }

    # Loop through the dictionary and print data
    print(api_data_dict.items())
    for entity_key, entity_value in api_data_dict.items():
        print("*" *30)
        print(f"Printing data for: {entity_key}")
        print("*" *30)
        for category_key, category_value in entity_value.items():

            function_name = category_value.get('function_name', '')
            data_type = category_value.get('data_type', '')
            reporting_limit = category_value.get('reporting_limit', '')
            headers = category_value.get('headers', {})
            params = category_value.get('params', {})
            api_url = category_value.get('api_url', '')
            return_key = category_value.get('return_key', '')
            sub_folder_template = category_value.get('sub_folder_template', '')
            file_name_template = category_value.get('file_name_template', '')
            output_csv_files = category_value.get('output_csv_files', '')
            normalize_json = category_value.get('normalize_json', '')
            record_path_for_normalize_json = category_value.get('record_path_for_normalize_json', '')
            json_normalize_keys = category_value.get('json_normalize_keys', {})
            meta_for_normalized_json = category_value.get('meta_for_normalized_json', {})
            normalized_concatenation_keys = category_value.get('normalized_concatenation_keys', {})
            json_explode = category_value.get('json_explode', '')
            removed_data_lists = category_value.get('removed_data_lists', '')
            special_note = category_value.get('special_note', '')
            run_option = category_value.get('run_option', '')
            consolidate_files = category_value.get('consolidate_files', '') 
            output_consolidated_csv_files = category_value.get('output_consolidated_csv_files', '')

            print("----------------------------------")
            print(f" Function Name: {function_name}")
            print(f" Data Type: {data_type}")
            print(f" Reporting Limit in Days: {data_type}")
            print(f" Headers: {headers}")
            print(f" Params: {params}")
            print(f" URL: {api_url}")
            print(f" Return Key: {return_key}")
            print(f" Sub Folder: {sub_folder_template}")
            print(f" File Name: {file_name_template}")
            print(f" Output CSV Files: {output_csv_files}")
            print(f" Normalize JSON: {normalize_json}")
            print(f" Record Path for Normalize JSON Data: {record_path_for_normalize_json}")
            print(f" Keys for Normalize JSON Data: {json_normalize_keys}")
            print(f" Meta Data for Normalize JSON: {meta_for_normalized_json}")
            print(f" Concatenation Keys for Normalize JSON: {normalized_concatenation_keys}")
            print(f" Explode JSON Data :{json_explode}")
            print(f" Removed Data Lists :{removed_data_lists}")
            print(f" Special Note: {special_note}")
            print(f" Run Option: {run_option}")
            print(f" Consolidate Files: {consolidate_files}")
            print(f" Output Consolidated CSV Files: {output_consolidated_csv_files}")
            print("----------------------------------")


    return api_data_dict