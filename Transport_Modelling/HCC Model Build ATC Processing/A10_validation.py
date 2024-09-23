import pandas as pd
import File_directories
import Create_DBs
from typing import List
import os
import glob
import re
import matplotlib.pyplot as plt
import seaborn as sns
import datetime

"""
Used for count comparison of Aecom's 2014 data within the Broxbourne A10 MRN scheme business case study area

required outputs are average AM, PM and IP period flows
"""

def filter_a10_validation_sites(input_dir: str, years: list, filter: str, output_dir: str):
    """
    filters required sites from outlier filtered database (combined into hourly intervals)
    """
    
    os.chdir(input_dir)

    #site filter taken off of Aecom validation sheet - contains additional info such as SATURN link reference
    #create site, source, direction reference for direction-specific saturn link correspondence. Drop unneeded columns
    site_filter = pd.read_csv(filter)
    site_filter['site - Source - direction'] = site_filter['site - Source'].astype(str) +" ATC" + " - " + site_filter["direction"].astype(str)
    site_filter = site_filter.drop(columns = ["SiteNumber", "A-Node", "B-Node", "Source", "site - Source", "direction", "Validation direction"])
    
    #loop around years
    for year in years:
        #read ATC data corresponding to each year and define required lookup columns
        data = pd.read_csv(f"{year}\\ATC_{year}_Combined_filtered_outliers_database.csv")
        data["site - Source"] = data["site"].astype(str) + " - " + data["Source"].astype(str)
        data["site - Source - direction"] = data["site - Source"].astype(str) + " - " + data["direction"].astype(str)
        
        #merge to attach SATURN link reference and 2014 flows for each site by direction and source. output resulting dataframe
        filtered_data = pd.merge(data, site_filter, how = "inner", on = "site - Source - direction")
        filtered_data = filtered_data.drop(columns = ["site - Source - direction"])
        filtered_data.to_csv(f"{output_dir}\\ATC_{year}_A10_Validation_Sites.csv", index = False)

def average_flow_AM_IP_PM(input_dir: str, years: list, AM_peak: str, PM_peak: str, output_dir: str):
    """
    Calculates average flows for AM peak, PM peak and Interpeak
    """
    
    os.chdir(input_dir)
    full_avg_flow_db = pd.DataFrame()

    #loop through years
    for year in years:
        #read A10 site dataframe for specific year
        data = pd.read_csv(f"ATC_{year}_A10_Validation_Sites.csv")

        #define time column
        data["time"] = pd.to_datetime(data["date"], dayfirst = True).dt.time.astype(str)
        #average flows by site, source, direction, and time. Keep SATURN link reference
        average_flows = data.groupby(["Ref", 'site - Source','direction','time']).mean()
        
        #output result for specific year, and add to full A10 validation database
        average_flows["year"] = year
        average_flows = average_flows.reset_index()
        average_flows.to_csv(f"{output_dir}\\ATC_{year}_A10_Validation_average_flows.csv", index = False)
        print(average_flows)
        full_avg_flow_db = pd.concat([full_avg_flow_db, average_flows])
    print(full_avg_flow_db)

    #define empty dataframe to store AM, PM and IP values in
    final_db = pd.DataFrame()
    
    #loop through A10 sites
    for site in full_avg_flow_db["site - Source"].unique():
        #create individual dataframe for site
        sitedf = full_avg_flow_db.loc[full_avg_flow_db["site - Source"] == site]
        
        #store AM peak row of data, specifying time period
        AM_peak_flows = sitedf.loc[sitedf["time"] == AM_peak]
        AM_peak_flows["time"] = "AM"

        #calculate average hourly flow within IP period and store result, specifying time period
        IP_flows = sitedf.loc[sitedf['time'].isin([str(datetime.time(hour = 10, minute = 0, second = 0)),str(datetime.time(hour = 11, minute = 0, second = 0)),str(datetime.time(hour = 12, minute = 0, second = 0)),str(datetime.time(hour = 13, minute = 0, second = 0)),str(datetime.time(hour = 14, minute = 0, second = 0)),str(datetime.time(hour = 15, minute = 0, second = 0))])]
        IP_avg_hour_flow = IP_flows.groupby(["Ref", 'site - Source','direction',"year"]).mean()
        IP_avg_hour_flow = IP_avg_hour_flow.reset_index()
        IP_avg_hour_flow["time"] = "IP"

        #store PM peak row of data, specifying time period
        PM_peak_flows = sitedf.loc[sitedf["time"] == PM_peak]
        PM_peak_flows["time"] = "PM"

        #append to final db
        final_db = pd.concat([final_db, AM_peak_flows, IP_avg_hour_flow, PM_peak_flows])

    #output results to csv
    final_db.to_csv(f"{output_dir}\\ATC_peak_hour_flows_by_year_A10_validation.csv", index = False)








if __name__ == "__main__":

    #filter_a10_validation_sites(File_directories.ATC_COMBINED_OUTLIER_FILTERED,["2019", "2022", "2023"],File_directories.A10_VALIDATION_SITE_FILTER,File_directories.A10_VALIDATION_FULL_DB_DIRECTORY)
    average_flow_AM_IP_PM(File_directories.A10_VALIDATION_FULL_DB_DIRECTORY,["2019", "2022", "2023"], "08:00:00", "17:00:00", File_directories.A10_VALIDATION_AVERAGE_FLOW_DIRECTORY)
