# -*- coding: utf-8 -*-
"""
Created on Wed Mar  6 18:13:45 2024

@author: INAJ05193
"""

import pandas as pd
import datetime
from datetime import timedelta

peak_hour_db = pd.read_csv(r"C:\Users\INVB05915\Documents\Projects\10 SCTM\Annualisation Factors\b_Expan_Neutral to Everyday\Database\ATC_2023_15m_appended_neutral_mon-thurs_filtered_outliers_database.csv")

peak_period_db = pd.read_csv(r"C:\Users\INVB05915\Documents\Projects\10 SCTM\Annualisation Factors\b_Expan_Neutral to Everyday\Database\ATC_2023_15m_appended_neutral_mon-sun_filtered_outliers_database.csv")

peak_hour_db["time"] = pd.to_datetime(peak_hour_db["date"]).dt.time.astype(str)
peak_hour_db["Date"] = pd.to_datetime(peak_hour_db["date"]).dt.date
peak_hour_db["hour"] = peak_hour_db["time"].apply(lambda x: datetime.datetime.strptime(x, "%H:%M:%S").hour)
peak_hour_db["hour"] = peak_hour_db["hour"].apply(lambda x: datetime.datetime.strftime(datetime.datetime.strptime(str(x), "%H"), "%H:%M:%S"))
peak_hour_db['site - Source'] = peak_hour_db['site'].astype(str).str.cat(peak_hour_db['Source'], sep='-')
columns_to_mean = [col for col in peak_hour_db.columns if col not in ['date','site','Source','site - Source','direction','time','hour','Date']]
peak_hour_db = peak_hour_db.groupby(['site - Source','direction','hour','Date','site'])[columns_to_mean].sum()
peak_hour_db = peak_hour_db.groupby(['site - Source','direction','hour','site'])[columns_to_mean].mean()
peak_hour_db.reset_index(inplace=True)


peak_period_db["time"] = pd.to_datetime(peak_period_db["date"]).dt.time.astype(str)
peak_period_db["Date"] = pd.to_datetime(peak_period_db["date"]).dt.date
peak_period_db["hour"] = peak_period_db["time"].apply(lambda x: datetime.datetime.strptime(x, "%H:%M:%S").hour)
peak_period_db["hour"] = peak_period_db["hour"].apply(lambda x: datetime.datetime.strftime(datetime.datetime.strptime(str(x), "%H"), "%H:%M:%S"))
peak_period_db['site - Source'] = peak_period_db['site'].astype(str).str.cat(peak_period_db['Source'], sep='-')
columns_to_mean = [col for col in peak_period_db.columns if col not in ['date','site','Source','site - Source','direction','time','hour','Date']]
peak_period_db = peak_period_db.groupby(['site - Source','direction','hour','Date','site'])[columns_to_mean].sum()
peak_period_db = peak_period_db.groupby(['site - Source','direction','hour','site'])[columns_to_mean].mean()
peak_period_db.reset_index(inplace=True)


AM_peak = "08:00:00"
PM_peak = "17:00:00"






AM_peak_flow = peak_hour_db.loc[peak_hour_db["hour"] == AM_peak]
AM_peak_flow = AM_peak_flow.groupby(['site','direction'])[['flow_car','flow_LGV', 'flow_HGV','flow_BUS']].sum()
AM_peak_flow["time"] = "AM"
AM_peak_flow.reset_index(inplace=True)

AM_period_flow = peak_period_db.loc[peak_period_db['hour'].isin([str(datetime.time(hour = 7, minute = 0, second = 0)),str(datetime.time(hour = 8, minute = 0, second = 0)),str(datetime.time(hour = 9, minute = 0, second = 0))])]
AM_period_flow = AM_period_flow.groupby(['site','direction'])[['flow_car','flow_LGV', 'flow_HGV','flow_BUS']].sum()
AM_period_flow["time"] = "AM"
AM_period_flow.reset_index(inplace=True)
print(AM_period_flow)

merged_df_1 = pd.merge(AM_period_flow, AM_peak_flow, on=['site', 'direction'], suffixes=('_period', '_peak'))
merged_df_1['Total_Flow_Period']= merged_df_1['flow_car_period']+merged_df_1['flow_LGV_period']+merged_df_1['flow_HGV_period']+merged_df_1['flow_BUS_period']

merged_df_1['Total_PrT_Flow_Period']= merged_df_1['flow_car_period']+merged_df_1['flow_LGV_period']+merged_df_1['flow_HGV_period']

merged_df_1['Total_Flow_Peak']= merged_df_1['flow_car_peak']+merged_df_1['flow_LGV_peak']+merged_df_1['flow_HGV_peak']+merged_df_1['flow_BUS_peak']

merged_df_1['Total_PrT_Flow_Peak']= merged_df_1['flow_car_peak']+merged_df_1['flow_LGV_peak']+merged_df_1['flow_HGV_peak']

merged_df_1['Expan_neutral_to_weekday_car'] = merged_df_1['flow_car_period']/merged_df_1['flow_car_peak']
merged_df_1['Expan_neutral_to_weekday_LGV'] = merged_df_1['flow_LGV_period']/merged_df_1['flow_LGV_peak']
merged_df_1['Expan_neutral_to_weekday_HGV'] = merged_df_1['flow_HGV_period']/merged_df_1['flow_HGV_peak']
merged_df_1['Expan_neutral_to_weekday_BUS'] = merged_df_1['flow_BUS_period']/merged_df_1['flow_BUS_peak']
merged_df_1['Expan_neutral_to_weekday_total'] = merged_df_1['Total_Flow_Period']/merged_df_1['Total_Flow_Peak']
merged_df_1['Expan_neutral_to_weekday_PrT_total'] = merged_df_1['Total_PrT_Flow_Period']/merged_df_1['Total_PrT_Flow_Peak']

merged_df_1.to_csv(r"C:\Users\INVB05915\Documents\Projects\10 SCTM\Annualisation Factors\b_Expan_Neutral to Everyday\NeutralPkHr_to_WeekdayPkPrd_Mon-Sun_AM_v2.csv", index = False)
#merged_df[['Expansion_Fac_car', 'Expansion_Fac_LGV', 'Expansion_Fac_HGV', 'Expansion_Fac_BUS']] = merged_df[['flow_car_period', 'flow_LGV_period', 'flow_HGV_period', 'flow_BUS_period']].div(merged_df[['flow_car_peak', 'flow_LGV_peak', 'flow_HGV_peak', 'flow_BUS_peak']].values)

#Annualisation_AM = merged_df[['site','direction','time_period','Expansion_Fac_car', 'Expansion_Fac_LGV', 'Expansion_Fac_HGV', 'Expansion_Fac_BUS']]
#Annualisation_AM.rename(columns={'time_period': 'time','flow_car_period': 'flow_car', 'flow_LGV_period': 'flow_LGV', 'flow_HGV_period': 'flow_HGV', 'flow_BUS_period': 'flow_BUS'}, inplace=True)






PM_peak_flow = peak_hour_db.loc[peak_hour_db["hour"] == PM_peak]
PM_peak_flow = PM_peak_flow.groupby(['site','direction'])[['flow_car','flow_LGV', 'flow_HGV','flow_BUS']].sum()
PM_peak_flow["time"] = "PM"
PM_peak_flow.reset_index(inplace=True)

PM_period_flow = peak_period_db.loc[peak_period_db['hour'].isin([str(datetime.time(hour = 16, minute = 0, second = 0)),str(datetime.time(hour = 17, minute = 0, second = 0)),str(datetime.time(hour = 18, minute = 0, second = 0))])]
PM_period_flow = PM_period_flow.groupby(['site','direction'])[['flow_car','flow_LGV', 'flow_HGV','flow_BUS']].sum()
PM_period_flow["time"] = "PM"
PM_period_flow.reset_index(inplace=True)

merged_df_2 = pd.merge(PM_period_flow, PM_peak_flow, on=['site', 'direction'], suffixes=('_period', '_peak'))
merged_df_2['Total_Flow_Period']= merged_df_2['flow_car_period']+merged_df_2['flow_LGV_period']+merged_df_2['flow_HGV_period']+merged_df_2['flow_BUS_period']

merged_df_2['Total_PrT_Flow_Period']= merged_df_2['flow_car_period']+merged_df_2['flow_LGV_period']+merged_df_2['flow_HGV_period']

merged_df_2['Total_Flow_Peak']= merged_df_2['flow_car_peak']+merged_df_2['flow_LGV_peak']+merged_df_2['flow_HGV_peak']+merged_df_2['flow_BUS_peak']

merged_df_2['Total_PrT_Flow_Peak']= merged_df_2['flow_car_peak']+merged_df_2['flow_LGV_peak']+merged_df_2['flow_HGV_peak']

merged_df_2['Expan_neutral_to_weekday_car'] = merged_df_2['flow_car_period']/merged_df_2['flow_car_peak']
merged_df_2['Expan_neutral_to_weekday_LGV'] = merged_df_2['flow_LGV_period']/merged_df_2['flow_LGV_peak']
merged_df_2['Expan_neutral_to_weekday_HGV'] = merged_df_2['flow_HGV_period']/merged_df_2['flow_HGV_peak']
merged_df_2['Expan_neutral_to_weekday_BUS'] = merged_df_2['flow_BUS_period']/merged_df_2['flow_BUS_peak']
merged_df_2['Expan_neutral_to_weekday_total'] = merged_df_2['Total_Flow_Period']/merged_df_2['Total_Flow_Peak']
merged_df_2['Expan_neutral_to_weekday_PrT_total'] = merged_df_2['Total_PrT_Flow_Period']/merged_df_2['Total_PrT_Flow_Peak']

merged_df_2.to_csv(r"C:\Users\INVB05915\Documents\Projects\10 SCTM\Annualisation Factors\b_Expan_Neutral to Everyday\NeutralPkHr_to_WeekdayPkPrd_Mon-Sun_PM_v2.csv", index = False)
#merged_df[['flow_car_period', 'flow_LGV_period', 'flow_HGV_period', 'flow_BUS_period']] = merged_df[['flow_car_period', 'flow_LGV_period', 'flow_HGV_period', 'flow_BUS_period']].div(merged_df[['flow_car_peak', 'flow_LGV_peak', 'flow_HGV_peak', 'flow_BUS_peak']].values)

#Annualisation_PM = merged_df[['site','direction','time_period','flow_car_period', 'flow_LGV_period', 'flow_HGV_period', 'flow_BUS_period']]
#Annualisation_PM.rename(columns={'time_period': 'time','flow_car_period': 'flow_car', 'flow_LGV_period': 'flow_LGV', 'flow_HGV_period': 'flow_HGV', 'flow_BUS_period': 'flow_BUS'}, inplace=True)






IP_peak_flow = peak_hour_db.loc[peak_hour_db['hour'].isin([str(datetime.time(hour = 10, minute = 0, second = 0)),str(datetime.time(hour = 11, minute = 0, second = 0)),str(datetime.time(hour = 12, minute = 0, second = 0)),str(datetime.time(hour = 13, minute = 0, second = 0)),str(datetime.time(hour = 14, minute = 0, second = 0)),str(datetime.time(hour = 15, minute = 0, second = 0))])]
columns_to_mean = [col for col in IP_peak_flow.columns if col not in ['date','site','Source','site - Source','direction','time','hour','Date']]
IP_peak_flow = IP_peak_flow.groupby(['site - Source','direction','site'])[['flow_car','flow_LGV', 'flow_HGV','flow_BUS']].mean()
IP_peak_flow["time"] = "IP"
IP_peak_flow.reset_index(inplace=True)

IP_period_flow = peak_period_db.loc[peak_period_db['hour'].isin([str(datetime.time(hour = 10, minute = 0, second = 0)),str(datetime.time(hour = 11, minute = 0, second = 0)),str(datetime.time(hour = 12, minute = 0, second = 0)),str(datetime.time(hour = 13, minute = 0, second = 0)),str(datetime.time(hour = 14, minute = 0, second = 0)),str(datetime.time(hour = 15, minute = 0, second = 0))])]
columns_to_mean = [col for col in IP_period_flow.columns if col not in ['date','site','Source','site - Source','direction','time','hour','Date']]
IP_period_flow = IP_period_flow.groupby(['site - Source','direction','site'])[['flow_car','flow_LGV', 'flow_HGV','flow_BUS']].sum()
IP_period_flow["time"] = "IP"
IP_period_flow.reset_index(inplace=True)

merged_df_3 = pd.merge(IP_period_flow, IP_peak_flow, on=['site', 'direction'], suffixes=('_period', '_peak'))
merged_df_3['Total_Flow_Period']= merged_df_3['flow_car_period']+merged_df_3['flow_LGV_period']+merged_df_3['flow_HGV_period']+merged_df_3['flow_BUS_period']

merged_df_3['Total_PrT_Flow_Period']= merged_df_3['flow_car_period']+merged_df_3['flow_LGV_period']+merged_df_3['flow_HGV_period']

merged_df_3['Total_Flow_Peak']= merged_df_3['flow_car_peak']+merged_df_3['flow_LGV_peak']+merged_df_3['flow_HGV_peak']+merged_df_3['flow_BUS_peak']

merged_df_3['Total_PrT_Flow_Peak']= merged_df_3['flow_car_peak']+merged_df_3['flow_LGV_peak']+merged_df_3['flow_HGV_peak']

merged_df_3['Expan_neutral_to_weekday_car'] = merged_df_3['flow_car_period']/merged_df_3['flow_car_peak']
merged_df_3['Expan_neutral_to_weekday_LGV'] = merged_df_3['flow_LGV_period']/merged_df_3['flow_LGV_peak']
merged_df_3['Expan_neutral_to_weekday_HGV'] = merged_df_3['flow_HGV_period']/merged_df_3['flow_HGV_peak']
merged_df_3['Expan_neutral_to_weekday_BUS'] = merged_df_3['flow_BUS_period']/merged_df_3['flow_BUS_peak']
merged_df_3['Expan_neutral_to_weekday_total'] = merged_df_3['Total_Flow_Period']/merged_df_3['Total_Flow_Peak']
merged_df_3['Expan_neutral_to_weekday_PrT_total'] = merged_df_3['Total_PrT_Flow_Period']/merged_df_3['Total_PrT_Flow_Peak']

merged_df_3.to_csv(r"C:\Users\INVB05915\Documents\Projects\10 SCTM\Annualisation Factors\b_Expan_Neutral to Everyday\NeutralPkHr_to_WeekdayPkPrd_Mon-Sun_IP_v2.csv", index = False)

#merged_df[['flow_car_period', 'flow_LGV_period', 'flow_HGV_period', 'flow_BUS_period']] = merged_df[['flow_car_period', 'flow_LGV_period', 'flow_HGV_period', 'flow_BUS_period']].div(merged_df[['flow_car_peak', 'flow_LGV_peak', 'flow_HGV_peak', 'flow_BUS_peak']].values)

#Annualisation_IP = merged_df[['site','direction','time_period','flow_car_period', 'flow_LGV_period', 'flow_HGV_period', 'flow_BUS_period']]
#Annualisation_IP.rename(columns={'time_period': 'time','flow_car_period': 'flow_car', 'flow_LGV_period': 'flow_LGV', 'flow_HGV_period': 'flow_HGV', 'flow_BUS_period': 'flow_BUS'}, inplace=True)


#Annualisation_Factor = pd.concat([Annualisation_AM, Annualisation_PM,Annualisation_IP])
#Annualisation_Factor.to_csv(r"C:\Users\INVB05915\Documents\Projects\10 SCTM\5 - Average_Flow\Annualisation_Factor.csv", index = False)

