import pandas as pd
import File_directories
import Create_DBs
from typing import List
import os
import glob
import re
import matplotlib.pyplot as plt
import seaborn as sns

def filter_ATC_By_Sample_Size(input_file: str, site_filter: str, output_dir: str, sample_size: int):
    data = pd.read_csv(input_file)
    site_filter_df = pd.read_csv(site_filter)
    data["site-Source"] = data["site"].astype(str) + " - " + data["Source"]
    filtered_data = pd.merge(data, site_filter_df, how = "inner", on = "site-Source")
    print(input_file)
    file_name = re.findall('\\\\\S+?\.',input_file)[0][:-1]
    print(file_name)
    filtered_data.to_csv(f"{output_dir}\\{file_name}_{sample_size}.csv", index = False)

def peak_hour_analysis(input_db: str, output_dir: str, year: str, output_filename: str, sample_size: str):
    """ Performs peak hour analysis on the input file. Results all output into the output directory"""
    Data = pd.read_csv(input_db)

    input_db_name = re.findall("ATC\S+.", input_db)[-1][:-1]
    print(input_db_name)
    os.chdir(output_dir)

    Data = Data.drop(columns = ['zscore_time'])

    Data['time'] = pd.to_datetime(Data['date'],dayfirst = True).dt.time

    Data_avg = Data.groupby(['site-Source','direction','time']).mean()
    Data_Two_Way = Data_avg.groupby(['site-Source','time']).sum().drop(columns = ['year'])

    Data_Two_Way = Data_Two_Way.reset_index()

    Data_Two_Way.to_csv(f"{output_dir}\\{output_filename}_Two_Way_Avg_Flows.csv", index = False)

    Data_Two_Way = Data_Two_Way.reset_index()
    hours = Data_Two_Way['time'].unique()

    peak_hour_df = pd.DataFrame(columns = ['site-Source', 'AM_peak', 'AM_peak_flow','PM_peak','PM_peak_flow'])

    for site in Data_Two_Way['site-Source'].unique():
        df_site = Data_Two_Way.loc[Data_Two_Way["site-Source"] == site]
        df_site['Hourly_Flow'] =  df_site['flow_total'].rolling(4).sum().shift(periods = -3)

        df_site['dummy_datetime'] = "01/02/2022 " + df_site['time'].astype(str)
        
        df_site['dummy_datetime'] = pd.to_datetime(df_site['dummy_datetime'],dayfirst = True)

        
        df_site = df_site.set_index('dummy_datetime')
        peak_hour_site_df = pd.DataFrame()
        
        
        AM_site_df = df_site.between_time("07:00:00","10:00:00")
        PM_site_df = df_site.between_time("16:00:00","19:00:00")

        AM_site_peak = 0

        for x in range(len(AM_site_df['time'])):
            if AM_site_df.iloc[x,9] > AM_site_peak:
                #print("Test")
                AM_site_peak = AM_site_df.iloc[x,9]
                AM_peak_hour = AM_site_df['time'][x]
                #print(AM_site_peak)
        
        PM_site_peak = 0
        for x in range(len(PM_site_df['time'])):
            if PM_site_df.iloc[x,9] > PM_site_peak:
                PM_site_peak = PM_site_df.iloc[x,9]
                PM_peak_hour = PM_site_df['time'][x]

        #peak_hour_site_df['site'] = site
        #peak_hour_site_df['AM_peak'] = AM_peak_hour
        #peak_hour_site_df['AM_peak_flow'] = AM_site_peak
        

        #peak_hour_site_df['PM_peak'] = PM_peak_hour
        #peak_hour_site_df['PM_peak_flow'] = PM_site_peak

        site_data = {"site-Source": [site], "AM_peak": [AM_peak_hour], "AM_peak_hour_flow": [AM_site_peak], "PM_peak": [PM_peak_hour], "PM_peak_hour_flow": [PM_site_peak]}
        site_data = pd.DataFrame(site_data)
        
        print("Test 1")
        maxflow_site = df_site["Hourly_Flow"].max()
        df_site['time'] = df_site['time'].astype(dtype = str)
        df_site = df_site.reset_index()
        g= sns.relplot(x="time",y="Hourly_Flow",kind="line",data=df_site,height=8.27, aspect=25/8,sort=False)
        
        print("Test 2")
        peak_hour_df = pd.concat([peak_hour_df,site_data])

        g.set(ylim=(0,maxflow_site))
        g.set(ylabel="Flow (veh/h)")
        #sns.catplot(x="month",y="flow",data=df,kind="box", whis=1.5)
        #plt.show()
        print("Test 3")
        z = "Site "+str(site)+f" ATC average survey flow {year}"
        plt.title(z)
        print("Test 4")
        plt.savefig(z)
        plt.close('all')

    print("saved all site figures")
    peak_hour_df.to_csv(f"{output_dir}\\{output_filename}_Peak_hours_by_site.csv")

    total_hourly_flow_24h = Data_Two_Way.groupby(["time"]).sum().reset_index()
    total_hourly_flow_24h['Hourly_Flow'] =  total_hourly_flow_24h['flow_total'].rolling(4).sum().shift(periods = -3)
    total_hourly_flow_24h.to_csv(f"{output_filename}_Total_Flow_24h_PH_Analysis_{sample_size}_days.csv")
    maxflow = total_hourly_flow_24h["Hourly_Flow"].max()
    total_hourly_flow_24h['time'] = total_hourly_flow_24h['time'].astype(dtype = str)
    g=sns.relplot(x="time",y="Hourly_Flow",kind="line",data=total_hourly_flow_24h,height=8.27, aspect=25/8,sort=False)
    g.set(ylim=(0,maxflow))
    g.set(ylabel="Flow (veh/h)")
    print("saving overall curve")
    #sns.catplot(x="month",y="flow",data=df,kind="box", whis=1.5)
    #plt.show()
    
    z = f"{output_filename} Total Average Hourly Flow"
    plt.title(z)       
    plt.savefig(z)

if __name__ == "__main__":
    os.chdir(os.getcwd())
    #filter_ATC_By_Sample_Size(f"{File_directories.ATC_OUTLIER_FILTERED}\\2023\\ATC_2023_15m_appended_neutral_mon-thurs_filtered_outliers_database.csv",File_directories.SAMPLE_FILTER_8,File_directories.PRE_PEAK_HOUR_DBS,8)
    #filter_ATC_By_Sample_Size(f"{File_directories.ATC_OUTLIER_FILTERED}\\2023\\ATC_2023_15m_appended_neutral_mon-thurs_filtered_outliers_database.csv",File_directories.SAMPLE_FILTER_6,File_directories.PRE_PEAK_HOUR_DBS,6)
    #post_filter = pd.read_csv(f"{File_directories.PROGRAM_DIRECTORY}\\4 - Filtered_DBs\\2023\\ATC_15m_appended_neutral_mon-thurs._filtered_outliers_database.csv")
    #pre_filter = pd.read_csv(f"{File_directories.PROGRAM_DIRECTORY}\\{File_directories.ATC_MON_THURS_NEUTRAL}\\2023\\ATC_2023_15m_appended_neutral_mon-thurs.csv")

    #peak_hour_analysis(File_directories.PEAK_HOUR_INPUT_2023_6DAYS,File_directories.PEAK_HOUR_2023,"2023", "2023_ATC_HCC_Severnside_6Days", "6")
    #peak_hour_analysis(File_directories.PEAK_HOUR_INPUT_2023_8DAYS,File_directories.PEAK_HOUR_2023,"2023", "2023_ATC_HCC_Severnside_8Days", "8")