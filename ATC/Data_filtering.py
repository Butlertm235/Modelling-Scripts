# -*- coding: utf-8 -*-
"""
Created on Wed Jun 10 08:35:39 2020

@author: UKECF002
"""

import os
import pandas as pd
import numpy as np

#Create an empty database
filtered_database = pd.DataFrame()

#Filter by AM, IP and PM
tp_filter = pd.read_csv("time_period.csv")

#Filter database to exclude Fri,Sat,Sun, Bank Holidays and School Holidays
day_filter = pd.read_csv("day_filter_2019_TAG_neutral.csv",parse_dates=["date_1"],dayfirst = True)
day_filter["date_1"] = day_filter["date_1"].dt.date

db_path = r"\\uk.wspgroup.com\Central Data\Projects\70092xxx\70092907 - SCTM - 2019 PYV\03 WIP\TP Transport Planning\01 Analysis & Calcs\04_Data\01_Count_Data\10_SCTM_2019_PYV_ATC_DB\SCTM_2019_PYV_ATC_DB.csv"

#Read database
df = pd.read_csv(db_path,parse_dates=["date"],dayfirst = True)

#drop rows without site information (apply to WebTRIS data only)
df = df.dropna(subset=["site"])

df["hour"] = df.date.dt.hour
df["date_1"] = df.date.dt.date
df["month"] = df.date.dt.month

#Apply time period filter
df = pd.merge(df,tp_filter,how="inner",on="hour")

#Apply day filter
df = pd.merge(df,day_filter,how="inner",on="date_1")


#Generate matrix with maximum number of possible sample records
full_sample_size = df.groupby(["source","site","month"]).count()
full_sample_size_pivot = full_sample_size.pivot_table(index=["source","site"],columns =["month"],values = "date")

#Drop rows with no data
df = df.dropna(subset=["flow_total"])

#Generate matrix with sample records after removing blank data
not_null_sample_size = df.groupby(["source","site","month"]).count()
not_null_sample_size_pivot = not_null_sample_size.pivot_table(index=["source","site"],columns =["month"],values = "date")

#Generate matrix with data coverage percentage
data_coverage = not_null_sample_size_pivot/full_sample_size_pivot
data_coverage = data_coverage.fillna(0)

os.chdir(r"\\uk.wspgroup.com\Central Data\Projects\70092xxx\70092907 - SCTM - 2019 PYV\03 WIP\TP Transport Planning\01 Analysis & Calcs\04_Data\01_Count_Data\11_Data_processing\01_TAG_Neutral")

#Export the dataframe to a .csv file
export_csv = full_sample_size_pivot.to_csv("full_sample_size_pivot_filtered.csv")
export_csv = not_null_sample_size_pivot.to_csv("not_null_sample_size_pivot_filtered.csv")
export_csv = data_coverage.to_csv("data_coverage_filtered.csv")

df = df.drop(columns=["hour","date_1","month"])

export_csv = df.to_csv("filtered_database_2019_TAG_neutral.csv",index=False)