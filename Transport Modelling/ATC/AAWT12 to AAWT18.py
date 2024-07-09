# -*- coding: utf-8 -*-
"""
Created on Wed Mar 13 15:26:44 2024

@author: INVB05915 (Vishal Behera)
"""

import pandas as pd
import datetime
from datetime import timedelta

#Two database of the same file was created

aawt_12_db = pd.read_csv(r"C:\Users\INVB05915\Documents\Projects\10 SCTM\Annualisation Factors\c_AAWT12_to_AAWT18\Database\ATC_2023_15m_appended_neutral_mon-fri_filtered_outliers_database.csv") #Link could be changed in future

aawt_18_db = pd.read_csv(r"C:\Users\INVB05915\Documents\Projects\10 SCTM\Annualisation Factors\c_AAWT12_to_AAWT18\Database\ATC_2023_15m_appended_neutral_mon-fri_filtered_outliers_database.csv") #Link could be changed in future

aawt_12_db["time"] = pd.to_datetime(aawt_12_db["date"]).dt.time.astype(str) #Convert the 'date' column to datetime format and extract the time component, storing it as a string#
aawt_12_db["Date"] = pd.to_datetime(aawt_12_db["date"]).dt.date #Extract the date component from the 'date' column#

# Extract the hour component from the time string and convert it to a string format#
# Combine the 'site' and 'Source' columns to create a new column 'site - Source'.
# Identify columns to calculate the mean (average) for.
# Group the DataFrame by 'site - Source', 'direction', 'hour', 'Date', and 'site', and calculate the sum of values for selected columns.
# Group the DataFrame again by 'site - Source', 'direction', 'hour', and 'site', and calculate the mean of values for selected columns.
# Reset the index of the DataFrame.

aawt_12_db["hour"] = aawt_12_db["time"].apply(lambda x: datetime.datetime.strptime(x, "%H:%M:%S").hour) 
aawt_12_db["hour"] = aawt_12_db["hour"].apply(lambda x: datetime.datetime.strftime(datetime.datetime.strptime(str(x), "%H"), "%H:%M:%S"))
aawt_12_db['site - Source'] = aawt_12_db['site'].astype(str).str.cat(aawt_12_db['Source'], sep='-')
columns_to_mean = [col for col in aawt_12_db.columns if col not in ['date','site','Source','site - Source','direction','time','hour','Date']]
aawt_12_db = aawt_12_db.groupby(['site - Source','direction','hour','Date','site'])[columns_to_mean].sum()
aawt_12_db = aawt_12_db.groupby(['site - Source','direction','hour','site'])[columns_to_mean].mean()
aawt_12_db.reset_index(inplace=True)


aawt_18_db["time"] = pd.to_datetime(aawt_18_db["date"]).dt.time.astype(str)
aawt_18_db["Date"] = pd.to_datetime(aawt_18_db["date"]).dt.date
aawt_18_db["hour"] = aawt_18_db["time"].apply(lambda x: datetime.datetime.strptime(x, "%H:%M:%S").hour)
aawt_18_db["hour"] = aawt_18_db["hour"].apply(lambda x: datetime.datetime.strftime(datetime.datetime.strptime(str(x), "%H"), "%H:%M:%S"))
aawt_18_db['site - Source'] = aawt_18_db['site'].astype(str).str.cat(aawt_18_db['Source'], sep='-')
columns_to_mean = [col for col in aawt_18_db.columns if col not in ['date','site','Source','site - Source','direction','time','hour','Date']]
aawt_18_db = aawt_18_db.groupby(['site - Source','direction','hour','Date','site'])[columns_to_mean].sum()
aawt_18_db = aawt_18_db.groupby(['site - Source','direction','hour','site'])[columns_to_mean].mean()
aawt_18_db.reset_index(inplace=True)




hours_range = [str(datetime.time(hour=h, minute=0, second=0)) for h in range(7, 20)]
# This line creates a list called hours_range containing strings representing the hours from 7 AM (hour 7) to 7 PM (hour 19)

aawt_12_flow = aawt_12_db.loc[aawt_12_db['hour'].isin(hours_range)]
# This line selects rows from the DataFrame aawt_12_db where the 'hour' column matches any of the values in the hours_range
aawt_12_flow = aawt_12_flow.groupby(['site','direction'])[['flow_car','flow_LGV', 'flow_HGV','flow_BUS']].sum()

aawt_12_flow.reset_index(inplace=True)
print(aawt_12_flow)

# times represent the hours from 6 AM to 12 AM.
aawt_18_flow = aawt_18_db.loc[aawt_18_db['hour'].isin([
    str(datetime.time(hour=6, minute=0, second=0)),
    str(datetime.time(hour=7, minute=0, second=0)),
    str(datetime.time(hour=8, minute=0, second=0)),
    str(datetime.time(hour=9, minute=0, second=0)),
    str(datetime.time(hour=10, minute=0, second=0)),
    str(datetime.time(hour=11, minute=0, second=0)),
    str(datetime.time(hour=12, minute=0, second=0)),
    str(datetime.time(hour=13, minute=0, second=0)),
    str(datetime.time(hour=14, minute=0, second=0)),
    str(datetime.time(hour=15, minute=0, second=0)),
    str(datetime.time(hour=16, minute=0, second=0)),
    str(datetime.time(hour=17, minute=0, second=0)),
    str(datetime.time(hour=18, minute=0, second=0)),
    str(datetime.time(hour=19, minute=0, second=0)),
    str(datetime.time(hour=20, minute=0, second=0)),
    str(datetime.time(hour=21, minute=0, second=0)),
    str(datetime.time(hour=22, minute=0, second=0)),
    str(datetime.time(hour=23, minute=0, second=0))
])]


aawt_18_flow = aawt_18_flow.groupby(['site','direction'])[['flow_car','flow_LGV', 'flow_HGV','flow_BUS']].sum()

aawt_18_flow.reset_index(inplace=True)
print(aawt_18_flow)

# calculates various Annualization Factors between AAWT_18 and AAWT_12 for each type of vehicle and for total flows
# exports the merged  DataFrame to a CSV file 

merged_df = pd.merge(aawt_18_flow, aawt_12_flow, on=['site', 'direction'], suffixes=('_AAWT_18', '_AAWT_12'))
merged_df['Total_Flow_AAWT_18']= merged_df['flow_car_AAWT_18']+merged_df['flow_LGV_AAWT_18']+merged_df['flow_HGV_AAWT_18']+merged_df['flow_BUS_AAWT_18']

merged_df['Total_PrT_Flow_AAWT_18']= merged_df['flow_car_AAWT_18']+merged_df['flow_LGV_AAWT_18']+merged_df['flow_HGV_AAWT_18']

merged_df['Total_Flow_AAWT_12']= merged_df['flow_car_AAWT_12']+merged_df['flow_LGV_AAWT_12']+merged_df['flow_HGV_AAWT_12']+merged_df['flow_BUS_AAWT_12']

merged_df['Total_PrT_Flow_AAWT_12']= merged_df['flow_car_AAWT_12']+merged_df['flow_LGV_AAWT_12']+merged_df['flow_HGV_AAWT_12']


merged_df['AAWT12_to_AAWT18_Car'] = merged_df['flow_car_AAWT_18']/merged_df['flow_car_AAWT_12']
merged_df['AAWT12_to_AAWT18_LGV'] = merged_df['flow_LGV_AAWT_18']/merged_df['flow_LGV_AAWT_12']
merged_df['AAWT12_to_AAWT18_HGV'] = merged_df['flow_HGV_AAWT_18']/merged_df['flow_HGV_AAWT_12']
merged_df['AAWT12_to_AAWT18_BUS'] = merged_df['flow_BUS_AAWT_18']/merged_df['flow_BUS_AAWT_12']
merged_df['AAWT12_to_AAWT18_total'] = merged_df['Total_Flow_AAWT_18']/merged_df['Total_Flow_AAWT_12']
merged_df['AAWT12_to_AAWT18_PrT_total'] = merged_df['Total_PrT_Flow_AAWT_18']/merged_df['Total_PrT_Flow_AAWT_12']

merged_df.to_csv(r"C:\Users\INVB05915\Documents\Projects\10 SCTM\Annualisation Factors\c_AAWT12_to_AAWT18\AAWT12_to_AAWT18_Mon-Fri.csv", index = False) #Link could be changed in future




