"""
Created on Sun Mar 21 09:16:54 2021

@author: UKECF002

This script adds all the raw data files into a single dataframe and saves it to a csv file
"""

import atc

#INPUT
#Folder with the raw data downloaded from WebTRIS
data_folder = r"\\uk.wspgroup.com\Central Data\Projects\70092xxx\70092907 - SCTM - 2019 PYV\03 WIP\TP Transport Planning\01 Analysis & Calcs\04_Data\01_Count_Data\02_WebTRIS\01a_Raw_Data"
#Output file name
output = "WebTRIS_Suffolk_ATC_DB_2019"

db = atc.create_db_webtris(data_folder,output)

