

# -*- coding: utf-8 -*-
"""
Created on Wed Mar 13 15:26:44 2024

@author: INVB05915 (Vishal Behera)
"""

import pandas as pd
import datetime
from datetime import timedelta

#Two database of the same file was created

aadt_12_db = pd.read_csv(r"C:\Users\INVB05915\Documents\Projects\10 SCTM\Annualisation Factors\d_AADT12_to_AADT24\Database\ATC_2023_15m_appended_neutral_mon-sun_filtered_outliers_database.csv") #Link could be changed in future

aadt_24_db = pd.read_csv(r"C:\Users\INVB05915\Documents\Projects\10 SCTM\Annualisation Factors\d_AADT12_to_AADT24\Database\ATC_2023_15m_appended_neutral_mon-sun_filtered_outliers_database.csv") #Link could be changed in future

aadt_12_db["time"] = pd.to_datetime(aadt_12_db["date"]).dt.time.astype(str) #Convert the 'date' column to datetime format and extract the time component, storing it as a string#
aadt_12_db["Date"] = pd.to_datetime(aadt_12_db["date"]).dt.date #Extract the date component from the 'date' column#

# Extract the hour component from the time string and convert it to a string format#
# Combine the 'site' and 'Source' columns to create a new column 'site - Source'.
# Identify columns to calculate the mean (average) for.
# Group the DataFrame by 'site - Source', 'direction', 'hour', 'Date', and 'site', and calculate the sum of values for selected columns.
# Group the DataFrame again by 'site - Source', 'direction', 'hour', and 'site', and calculate the mean of values for selected columns.
# Reset the index of the DataFrame.

aadt_12_db["hour"] = aadt_12_db["time"].apply(lambda x: datetime.datetime.strptime(x, "%H:%M:%S").hour) 
aadt_12_db["hour"] = aadt_12_db["hour"].apply(lambda x: datetime.datetime.strftime(datetime.datetime.strptime(str(x), "%H"), "%H:%M:%S"))
aadt_12_db['site - Source'] = aadt_12_db['site'].astype(str).str.cat(aadt_12_db['Source'], sep='-')
columns_to_mean = [col for col in aadt_12_db.columns if col not in ['date','site','Source','site - Source','direction','time','hour','Date']]
aadt_12_db = aadt_12_db.groupby(['site - Source','direction','hour','Date','site'])[columns_to_mean].sum()
aadt_12_db = aadt_12_db.groupby(['site - Source','direction','hour','site'])[columns_to_mean].mean()
aadt_12_db.reset_index(inplace=True)


aadt_24_db["time"] = pd.to_datetime(aadt_24_db["date"]).dt.time.astype(str)
aadt_24_db["Date"] = pd.to_datetime(aadt_24_db["date"]).dt.date
aadt_24_db["hour"] = aadt_24_db["time"].apply(lambda x: datetime.datetime.strptime(x, "%H:%M:%S").hour)
aadt_24_db["hour"] = aadt_24_db["hour"].apply(lambda x: datetime.datetime.strftime(datetime.datetime.strptime(str(x), "%H"), "%H:%M:%S"))
aadt_24_db['site - Source'] = aadt_24_db['site'].astype(str).str.cat(aadt_24_db['Source'], sep='-')
columns_to_mean = [col for col in aadt_24_db.columns if col not in ['date','site','Source','site - Source','direction','time','hour','Date']]
aadt_24_db = aadt_24_db.groupby(['site - Source','direction','hour','Date','site'])[columns_to_mean].sum()
aadt_24_db = aadt_24_db.groupby(['site - Source','direction','hour','site'])[columns_to_mean].mean()
aadt_24_db.reset_index(inplace=True)




hours_range = [str(datetime.time(hour=h, minute=0, second=0)) for h in range(7, 20)]
# This line creates a list called hours_range containing strings representing the hours from 7 AM (hour 7) to 7 PM (hour 19)

aadt_12_flow = aadt_12_db.loc[aadt_12_db['hour'].isin(hours_range)]
# This line selects rows from the DataFrame aadt_12_db where the 'hour' column matches any of the values in the hours_range
aadt_12_flow = aadt_12_flow.groupby(['site','direction'])[['flow_car','flow_LGV', 'flow_HGV','flow_BUS']].sum()

aadt_12_flow.reset_index(inplace=True)
print(aadt_12_flow)

# times represent the 24 hrs 
aadt_24_flow = aadt_24_db.loc[aadt_24_db['hour'].isin([
    str(datetime.time(hour=h, minute=0, second=0)) for h in range(24)
])]



aadt_24_flow = aadt_24_flow.groupby(['site','direction'])[['flow_car','flow_LGV', 'flow_HGV','flow_BUS']].sum()

aadt_24_flow.reset_index(inplace=True)
print(aadt_24_flow)

# calculates various Annualization Factors between aadt_24 and aadt_12 for each type of vehicle and for total flows
# exports the merged  DataFrame to a CSV file 

merged_df = pd.merge(aadt_24_flow, aadt_12_flow, on=['site', 'direction'], suffixes=('_AADT_24', '_AADT_12'))
merged_df['Total_Flow_AADT_24']= merged_df['flow_car_AADT_24']+merged_df['flow_LGV_AADT_24']+merged_df['flow_HGV_AADT_24']+merged_df['flow_BUS_AADT_24']

merged_df['Total_PrT_Flow_AADT_24']= merged_df['flow_car_AADT_24']+merged_df['flow_LGV_AADT_24']+merged_df['flow_HGV_AADT_24']

merged_df['Total_Flow_AADT_12']= merged_df['flow_car_AADT_12']+merged_df['flow_LGV_AADT_12']+merged_df['flow_HGV_AADT_12']+merged_df['flow_BUS_AADT_12']

merged_df['Total_PrT_Flow_AADT_12']= merged_df['flow_car_AADT_12']+merged_df['flow_LGV_AADT_12']+merged_df['flow_HGV_AADT_12']


merged_df['AADT12_to_AADT24_Car'] = merged_df['flow_car_AADT_24']/merged_df['flow_car_AADT_12']
merged_df['AADT12_to_AADT24_LGV'] = merged_df['flow_LGV_AADT_24']/merged_df['flow_LGV_AADT_12']
merged_df['AADT12_to_AADT24_HGV'] = merged_df['flow_HGV_AADT_24']/merged_df['flow_HGV_AADT_12']
merged_df['AADT12_to_AADT24_BUS'] = merged_df['flow_BUS_AADT_24']/merged_df['flow_BUS_AADT_12']
merged_df['AADT12_to_AADT24_total'] = merged_df['Total_Flow_AADT_24']/merged_df['Total_Flow_AADT_12']
merged_df['AADT12_to_AADT24_PrT_total'] = merged_df['Total_PrT_Flow_AADT_24']/merged_df['Total_PrT_Flow_AADT_12']

merged_df.to_csv(r"C:\Users\INVB05915\Documents\Projects\10 SCTM\Annualisation Factors\d_AADT12_to_AADT24\AADT12_to_AADT24_Mon-Sun.csv", index = False) #Link could be changed in future




