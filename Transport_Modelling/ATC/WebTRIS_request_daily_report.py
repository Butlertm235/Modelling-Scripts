"""
Created on Mon Apr 11 18:08:53 2022

@author: UKECF002

This script donwload the daily reports (in 15-min intervals) for the selected WebTRIS sites and dates
"""

import pandas as pd
from pytris import API

#INPUTS
#List of WebTRIS sites to download
WebTRIS_sites = [13825,13826,13827,13919,13995,14013,14027]

#Start and end dates, as a string and in "DDMMYYYY" format
date_start ='01012019'
date_end ='31122019'

api = API()
daily = api.daily_reports()

appended = pd.DataFrame()



for i in WebTRIS_sites:
    try:
        result = daily.get(sites=str(i), start_date=date_start, end_date=date_end)
        df = result.to_frame()
        df["site"] = i
        df["Report Date"] = pd.to_datetime(df["Report Date"])
        export_csv = df.to_csv("WebTRIS_site_"+str(i)+".csv")
    except:
        pass
