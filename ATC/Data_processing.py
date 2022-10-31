
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 10 15:36:09 2020

@author: UKECF002
"""

import os
import pandas as pd
import numpy as np

db_path = r"\\uk.wspgroup.com\Central Data\Projects\70092xxx\70092907 - SCTM - 2019 PYV\03 WIP\TP Transport Planning\01 Analysis & Calcs\04_Data\01_Count_Data\11_Data_processing\02_Outlier_filtering\filtered_outliers_database_2.5.csv"


#Read filtered database
df = pd.read_csv(db_path,parse_dates=["date"],dayfirst = True,dtype={"direction":str})


#Calculate average flow per site, direction and time period
avge_flow = df.groupby(["source","site","direction","time_period"]).mean().reset_index()
avge_flow_pivot = avge_flow.pivot_table(index=["source","site","direction"],columns =["time_period"],values = "flow_total") 

os.chdir(r"\\uk.wspgroup.com\Central Data\Projects\70092xxx\70092907 - SCTM - 2019 PYV\03 WIP\TP Transport Planning\01 Analysis & Calcs\04_Data\01_Count_Data\11_Data_processing\03_Average_flows")

export_csv = avge_flow.to_csv("average_flow.csv",index=False)
#export_csv = avge_flow_pivot.to_csv("average_flow_pivot.csv")