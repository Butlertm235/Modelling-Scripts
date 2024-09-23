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






def convert_15m_db_to_hourly(data_15m: pd.DataFrame):

    data_15m['date'] = pd.to_datetime(data_15m['date'], dayfirst = True)
    data_15m['Date'] = data_15m['date'].dt.date
    data_15m['hour'] = data_15m['date'].dt.time.astype(str)
    np_hour_convert = lambda x: datetime.datetime.strptime(x[0:2]+":00:00",  "%H:00:00")
    data_15m['hour'] = data_15m['hour'].apply(np_hour_convert)
    data_15m['hour'] = data_15m['hour'].dt.time
    print(data_15m['hour'])
    hourly_data_table = data_15m.groupby(["site", "Source", "direction","Date", "hour"]).sum()

    hourly_data_table = hourly_data_table.reset_index()
    return hourly_data_table


def convert_15m_to_rolling_hour(data: str):
    x = 1

def filter_ATC_By_Sample_Size(input_file: str, site_filter: str, output_dir: str, sample_size: int):
    data = pd.read_csv(input_file)
    site_filter_df = pd.read_csv(site_filter)
    data["site-Source"] = data["site"].astype(str) + " - " + data["Source"]
    filtered_data = pd.merge(data, site_filter_df, how = "inner", on = "site-Source")
    print(input_file)
    file_name = re.findall('\\\\\S+?\.',input_file)[0][:-1]
    print(file_name)
    filtered_data.to_csv(f"{output_dir}\\{file_name}_{sample_size}.csv", index = False)



def attach_roadtypes(input_db: pd.DataFrame):
    road_type_lookup = pd.read_csv(File_directories.ATC_ROADTYPE_LOOKUP)
    data = pd.merge(input_db, road_type_lookup, on = "site - Source", how = "inner")
    return data

def calc_AM_IP_PM_Flows(data: pd.DataFrame, AM_peak_hour: str, PM_peak_hour: str):
    """finds all AM, IP and PM flows for a dataframe. data should already be in rolling hour format"""
    AM_peak_flows = data.loc[data["time"] == AM_peak_hour]
    AM_peak_flows["time_period"] = "AM"
    #print(AM_peak_flows)
    IP_flows = data.loc[data['time'].isin([str(datetime.time(hour = 10, minute = 0, second = 0)),str(datetime.time(hour = 11, minute = 0, second = 0)),str(datetime.time(hour = 12, minute = 0, second = 0)),str(datetime.time(hour = 13, minute = 0, second = 0)),str(datetime.time(hour = 14, minute = 0, second = 0)),str(datetime.time(hour = 15, minute = 0, second = 0))])]
    IP_avg_hour_flow = IP_flows.groupby(["Road type", "month"]).mean()
    IP_avg_hour_flow = IP_avg_hour_flow.reset_index()
    IP_avg_hour_flow["time_period"] = "IP"
    #print(IP_avg_hour_flow)
    PM_peak_flows = data.loc[data["time"] == PM_peak_hour]
    PM_peak_flows["time_period"] = "PM"
    #print(PM_peak_flows)

    output = pd.concat([AM_peak_flows, IP_avg_hour_flow, PM_peak_flows])
    return output

def calc_RT_AM_IP_PM_Flows_15m_intervals(data: pd.DataFrame, time_intervals: dict):
    """finds all AM, IP and PM flows for a dataframe. data should already be in rolling hour format"""
    AM_peak_flows = data.loc[data["time"].isin(time_intervals["AM"])]
    AM_peak_flows["time_period"] = "AM"
    AM_peak_hour_flows = AM_peak_flows.groupby(["Road type", "year", "month", "time_period"]).sum()
    AM_peak_hour_flows = AM_peak_hour_flows.reset_index()

    #print(AM_peak_flows)
    IP_flows = data.loc[data['time'].isin(time_intervals["IP"])]
    IP_flows["hour"] = IP_flows["time"].astype(str)
    np_hour_convert = lambda x: datetime.datetime.strptime(x[0:2]+":00:00",  "%H:00:00")
    IP_flows['hour'] = IP_flows['hour'].apply(np_hour_convert)
    IP_flows['hour'] = IP_flows['hour'].dt.time
    IP_rolling_hour_flows = IP_flows.groupby(["Road type","year", "month", "hour"]).sum()
    IP_rolling_hour_flows = IP_rolling_hour_flows.reset_index()

    IP_avg_hour_flow = IP_flows.groupby(["Road type","year", "month"]).mean()
    IP_avg_hour_flow = IP_avg_hour_flow.reset_index()
    IP_avg_hour_flow["time_period"] = "IP"
    #print(IP_avg_hour_flow)
    PM_peak_flows = data.loc[data["time"].isin(time_intervals["PM"])]
    PM_peak_flows["time_period"] = "PM"
    PM_peak_hour_flows = PM_peak_flows.groupby(["Road type", "year", "month", "time_period"]).sum()
    PM_peak_hour_flows = PM_peak_hour_flows.reset_index()
    #print(PM_peak_flows)

    output = pd.concat([AM_peak_hour_flows, IP_avg_hour_flow, PM_peak_hour_flows])
    return output
