import pandas as pd
import os

input_path = r'C:\Users\kaczanor\OneDrive - Enbridge Inc\Documents\Python\Revised-AESO-API-master\aggregated_hourly_import_export_data.csv'
output_path = r'C:\Users\kaczanor\OneDrive - Enbridge Inc\Documents\Python\Revised-AESO-API-master'
output_file_name = 'aggregated_hourly_import_export_data_sorted.csv'

# Load File
df = pd.read_csv(input_path)
print(f" df.head() at loading: {df.head()}")

# Convert begin_date_mpt to datetime
df['begin_date_mpt'] = pd.to_datetime(df['begin_date_mpt'], format='%m/%d/%Y %H:%M')

# Sort by begin_date_mpt
df_sorted = df.sort_values(by='begin_date_mpt', ascending=True)

print(f"df_sorted.head():\n{df_sorted.head()}")

# Save the sorted DataFrame to a new CSV file
output_path = os.path.join(output_path, output_file_name)
df_sorted.to_csv(output_path, index=False)

print(f"Sorted data saved to {output_path}")

