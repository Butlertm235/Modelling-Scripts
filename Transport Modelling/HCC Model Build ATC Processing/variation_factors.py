import pandas as pd
import File_directories
import Create_DBs
from typing import List
import os
import glob
import re
import matplotlib.pyplot as plt
import seaborn as sns
import util
import datetime


def average_flows_by_site_month_and_year(data: pd.DataFrame):

    data["month"] = pd.to_datetime(data['date'], dayfirst = True).dt.month
    data["site - Source - direction"] = data["site - Source"] + " - " + data["direction"]
    
    average_flows_by_month = data.groupby(["site", "Source", "site - Source","site - Source - direction", "direction", "Road type","year", "month", "time"]).mean()
    average_flows_by_month = average_flows_by_month.reset_index()
    return average_flows_by_month

def average_two_way_flows_by_site_month_and_year(data: pd.DataFrame):

    data["month"] = pd.to_datetime(data['date'], dayfirst = True).dt.month
    data["site - Source - direction"] = data["site - Source"] + " - " + data["direction"]
    
    average_flows_by_month = data.groupby(["site", "Source", "site - Source","site - Source - direction", "direction", "Road type","year", "month", "time"]).mean()
    average_flows_by_month = average_flows_by_month.reset_index()
    average_two_way_flows_by_month = average_flows_by_month.groupby(["site", "Source", "site - Source", "Road type","year", "month", "time"]).sum()
    average_two_way_flows_by_month = average_two_way_flows_by_month.reset_index()
    return average_two_way_flows_by_month


def monthly_variation_factor(input_dir: str, output_dir: str, year: str, target_month: int, time_intervals: dict):
    """Takes data for each month for one particular year and finds the conversion factors for average flows for
       AM, IP, PM by  road type, and returns a dictionary with the included information"""
    data = pd.read_csv(f"{input_dir}\\ATC_{year}_Sample_Filtered_DB.csv")
    data = util.attach_roadtypes(data)

    average_two_way_flows_by_month = average_two_way_flows_by_site_month_and_year(data)
    
    #print(average_two_way_flows_by_month)
    average_flows_by_RT_month = average_two_way_flows_by_month.groupby(["Road type","year", "month", "time"]).mean()
    average_flows_by_RT_month = average_flows_by_RT_month.reset_index()
    average_flows_by_RT_month = average_flows_by_RT_month.drop(columns = ["site"])
    #print("Test 1")
    #print(average_flows_by_RT_month)
    
    #####Insert AM, IP and PM calculation CHANGE TO 15M Ver if necessary
    average_flows_by_RT_month_TP = util.calc_RT_AM_IP_PM_Flows_15m_intervals(average_flows_by_RT_month, time_intervals)
    #print(average_flows_by_RT_month_TP)
    #print("Test 2")
    #print(average_flows_by_RT_month_TP)
    
    average_flows_by_RT_month_TP.to_csv("Test2.csv")
    target_month_average_flows = average_flows_by_RT_month_TP.loc[average_flows_by_RT_month_TP["month"] == target_month]
    target_month_average_flows = target_month_average_flows.reset_index()
    target_month_average_flows = target_month_average_flows.rename(columns = {"flow_total": "target_month_flow_total"})
    #print("Test 3")
    #print(target_month_average_flows[["Road type", "year","time_period", "month","target_month_flow_total"]])

    average_flows_by_RT_month_TP = pd.merge(average_flows_by_RT_month_TP,target_month_average_flows[["Road type","year","time_period", "target_month_flow_total"]], how = "left", on = ["year", "Road type", "time_period"])
    average_flows_by_RT_month_TP["target_month_uplift_factor"] = average_flows_by_RT_month_TP["target_month_flow_total"] / average_flows_by_RT_month_TP["flow_total"]
    #print("Test 4")
    #print(average_flows_by_RT_month_TP[["Road type", "year","time_period", "month", "flow_total", "target_month_flow_total", "target_month_uplift_factor"]])
    average_flows_by_RT_month_TP.to_csv(f"{output_dir}\\ATC_{year}_monthly_variation_factors_by_road_type.csv", index = False)

    monthly_variation_lookup = average_flows_by_RT_month_TP.drop(columns = ["flow_car", "flow_LGV", "flow_HGV", "flow_OGV_1","flow_OGV_2", "flow_BUS", "flow_total", "target_month_flow_total"])

    monthly_variation_lookup.to_csv(f"{output_dir}\\ATC_{year}_monthly_variation_lookup.csv", index = False)
    return monthly_variation_factor



def calculate_monthly_variation_factors(input_dir: str, output_dir: str, years: list, target_month: int, time_periods: dict):
    variation_factors = {}
    for year in years:
        x = monthly_variation_factor(input_dir, output_dir, year, target_month, time_periods)
        variation_factors[year] = x


def calculate_yearly_variation_factors(input_dir: str, output_dir: str,base_year: str, target_year: str, target_month: int, AM_peak_hour: str, PM_peak_hour: str):
    years = [target_year, base_year]
    
    data_year_1 = pd.read_csv(f"{input_dir}\\ATC_{years[0]}_Sample_Filtered_DB.csv")
    data_year_1 = util.attach_roadtypes(data_year_1)

    data_year_2 = pd.read_csv(f"{input_dir}\\ATC_{years[0]}_Sample_Filtered_DB.csv")
    data_year_2 = util.attach_roadtypes(data_year_2)

    average_flows_by_month_1 = average_flows_by_site_month_and_year(data_year_1)
    average_flows_by_month_2 = average_flows_by_site_month_and_year(data_year_2)

    average_flows_target_month_1 = average_flows_by_month_1.loc[average_flows_by_month_1["month"] == target_month]
    average_flows_target_month_2 = average_flows_by_month_2.loc[average_flows_by_month_2["month"] == target_month]
    
    average_flows_by_RT_1 = average_flows_target_month_1.groupby(["Road type","year", "month", "time"]).mean()
    average_flows_by_RT_1 = average_flows_by_RT_1.reset_index()
    average_flows_by_RT_2 = average_flows_target_month_2.groupby(["Road type","year", "month", "time"]).mean()
    average_flows_by_RT_2 = average_flows_by_RT_2.reset_index()

    average_flows_by_RT_TP_1 = util.calc_RT_AM_IP_PM_Flows_15m_intervals(average_flows_by_RT_1, AM_peak_hour, PM_peak_hour)
    average_flows_by_RT_TP_2 = util.calc_RT_AM_IP_PM_Flows_15m_intervals(average_flows_by_RT_2, AM_peak_hour, PM_peak_hour)
    
    print(average_flows_by_RT_TP_1)

    #####To do: Drop unneeded columns from db1 and merge db2 to db1, only adding the total flow column from db1. Then calculate factor
    average_flows_by_RT_TP_1.to_csv("Test.csv")

    







if __name__ == "__main__":
    monthly_variation_factor(f"{File_directories.PRE_PEAK_HOUR_DBS}\\2023\\",File_directories.MONTHLY_VARIATION_FACTORS,"2023", 3,File_directories.TIME_PERIODS)
    #calculate_yearly_variation_factors()