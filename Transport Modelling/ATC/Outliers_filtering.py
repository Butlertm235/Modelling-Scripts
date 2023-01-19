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


os.getcwd()

db_path = r"\\uk.wspgroup.com\Central Data\Projects\70092xxx\70092907 - SCTM - 2019 PYV\03 WIP\TP Transport Planning\01 Analysis & Calcs\04_Data\01_Count_Data\11_Data_processing\01_TAG_Neutral\filtered_database_2019_TAG_neutral.csv"

#Read database
df = pd.read_csv(db_path,parse_dates=["date"],dayfirst = True)
df["month"] = df.date.dt.month

#Generate matrix with initial records before removing outliers
not_null_sample_size = df.groupby(["source","site","month"]).count()
not_null_sample_size_pivot = not_null_sample_size.pivot_table(index=["source","site"],columns =["month"],values = "date")


zscore = lambda x: abs((x-x.mean()))/x.std()

df["zscore_hour"] = df.groupby(["source","site","direction","time_period"])["flow_total"].transform(zscore)
df = df[(df["zscore_hour"]<= 4)| (df["zscore_hour"].isnull())]

df["zscore_year"] = df.groupby(["source","site","direction","time_period","month"])["flow_total"].transform(zscore)
df = df[(df["zscore_year"]<= 2.5) | (df["zscore_year"].isnull())]

record_filter = lambda x: x["flow"].count() > 3

#df = df.groupby(["site","direction","hour","year"]).filter(record_filter)


final_sample_size = df.groupby(["source","site","month"]).count()
final_sample_size_pivot = final_sample_size.pivot_table(index=["source","site"],columns =["month"],values = "date")                    
final_data_coverage = final_sample_size_pivot/not_null_sample_size_pivot
df = df.drop(columns=["count_point_id","zscore_hour","zscore_year","month"])
os.chdir(r"\\uk.wspgroup.com\Central Data\Projects\70092xxx\70092907 - SCTM - 2019 PYV\03 WIP\TP Transport Planning\01 Analysis & Calcs\04_Data\01_Count_Data\11_Data_processing\02_Outlier_filtering")

#Export the dataframe to a .csv file
export_csv = df.to_csv("filtered_outliers_database_2.5.csv",index=False)
export_csv = final_sample_size_pivot.to_csv("final_sample_size_pivot_2.5.csv")
export_csv = final_data_coverage.to_csv("Outlier_filter_percentage_2.5.csv")
