from src.utilities import create_regional_import_export_file
from datetime import date
from datetime import datetime
import pandas as pd

#Note this is a mock path which gets edited in the code 
new_path = r'C:\Users\kaczanor\OneDrive - Enbridge Inc\Documents\Python\Revised-AESO-API-master\output\Metered Volumes\FWD BUY.csv'


# #Converta all column headers to upper case
# df = pd.read_csv(new_path)  

# # Convert column headings to upper case
# df.columns = df.columns.str.upper()

# #Save back to folder
# df.to_csv(new_path, index=True)


#Reset current_date as the next function has a similar While loop like the
# Example string representations of dates and times
updated_start_date_str = "2024-01-01 00:00:00"
original_end_date_str = "2024-12-31 23:00:00"

# Convert strings to datetime objects
updated_start_date = datetime.strptime(updated_start_date_str, "%Y-%m-%d %H:%M:%S")
original_end_date = datetime.strptime(original_end_date_str, "%Y-%m-%d %H:%M:%S")

create_regional_import_export_file(new_path, updated_start_date, original_end_date)