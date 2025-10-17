import pandas as pd
import glob

'''
This code calculate forecast error between current period forecast and next period actual.
'''

# Special Note
# The glob module in Python is used to retrieve files/pathnames matching a specified pattern. The pattern rules of glob 
# follow standard Unix path expansion rules. It is very useful in cases when you need to process a list of files with 
# similar names or extensions, but located in different directories.

# Get a list of all file paths
#file_paths = glob.glob('output_data/Historical Pool Price/pool_price_data*.csv')
file_paths = glob.glob('C:/Users/kaczanor/OneDrive - Enbridge Inc/Documents/Python/Revised-AESO-API-master/output/Historical Pool Price/pool_price_data*.csv')

# Read each file into a DataFrame and store all DataFrames in a list
df_list = [pd.read_csv(file_path) for file_path in file_paths]

# Concatenate all DataFrames
df = pd.concat(df_list)

# Convert the 'begin_datetime_mpt' column to datetime
df['begin_datetime_mpt'] = pd.to_datetime(df['begin_datetime_mpt'])

# Set 'begin_datetime_mpt' as the index
df.set_index('begin_datetime_mpt', inplace=True)

#Add a rolling STD to the data frame
df['rolling_30day_std'] = df['pool_price'].rolling('30D').std()
annual_rolling_30day_std = df['rolling_30day_std'].resample('Y').mean()

#Add additional rolling average/std pool price metrics over shorter period
df['rolling_5day_avg'] = df['pool_price'].rolling('5D').mean()
df['rolling_5day_std'] = df['pool_price'].rolling('5D').std()
annual_rolling_5day_std = df['rolling_5day_std'].resample('Y').mean()

# Calculate forecast error between current period forecast and next period actual
#df['forecast_error'] = df['forecast_pool_price'] - df['pool_price']
df['forecast_error'] = df['forecast_pool_price'].shift(-1) - df['pool_price']

# Calculate annual Mean Absolute Error (MAE) in MWh
# Here, forecast_error is the difference between the forecasted pool price and the actual pool price. 
# The error is positive when the forecast is too high and negative when it's too low.
annual_mae = df['forecast_error'].resample('Y').mean().abs()

# We can express this in % terms too
# Calculate Mean Absolute Percentage Error (MAPE)
# Note "inf" values occur in hours with a pool_price of 0, so when you calculate the absolute percentage error (APE), 
# you're dividing by 0, which results in inf.  To address this, we can add a small constant value (like 0.001) to the 
# denominator to avoid division by 0. Here's how you can modify the MAPE calculation code:

df['abs_error'] = df['forecast_error'].abs()
df['ape'] = df['abs_error'] / (df['pool_price'] + 0.001)
mape = df['ape'].resample('Y').mean() * 100

# Calculate correlation between forecast error and rolling average pool price
# This will give you a number between -1 and 1. A positive number indicates a positive relationship (i.e., as the rolling average 
# increases, so does the forecast error), and a negative number indicates a negative relationship.
correlation_fe_30day_avg_poolprice = df['forecast_error'].corr(df['rolling_30day_avg'])
correlation_fe_5day_avg_poolprice = df['forecast_error'].corr(df['rolling_5day_avg'])

#Calculate the correlaton between the forecast and the rolling std dev
correlation_forecast_30day_std_poolprice = df['forecast_error'].corr(df['rolling_30day_std'])
correlation_forecast_5day_std_poolprice = df['forecast_error'].corr(df['rolling_5day_std'])

#calculate the correlation between the annual MAE and the annual average rolling standard deviation:
correlation_mae_30day_std = annual_mae.corr(annual_rolling_30day_std)
correlation_mae_5day_std = annual_mae.corr(annual_rolling_5day_std)

#calculated the correlation between the annual MAPE and the annual average rolling standard deviation:
correlation_mape_30day_std = mape.corr(annual_rolling_30day_std)
correlation_mape_5day_std = mape.corr(annual_rolling_5day_std)


print("Annual Mean Absolute Error: \n", annual_mae)
print("Annual Mean Absolute Error: \n", mape)

print("Correlation between hourly forecast error and 30-day avg pool price: ", correlation_fe_30day_avg_poolprice)
print("Correlation between hourly forecast error and 5-day avg pool price: ", correlation_fe_5day_avg_poolprice)

print("Correlation between hourly forecast error and 30-day std pool price: ", correlation_forecast_30day_std_poolprice)
print("Correlation between hourly forecast error and 5-day std pool price: ", correlation_forecast_5day_std_poolprice)

print("Correlation between annual MAE and 30-day rolling std: ", correlation_mae_30day_std)
print("Correlation between annual MAPE and 30-day rolling std: ", correlation_mape_30day_std)

print("Correlation between annual MAE and 5-day rolling std: ", correlation_mae_5day_std)
print("Correlation between annual MAPE and 5-day rolling std: ", correlation_mape_5day_std)