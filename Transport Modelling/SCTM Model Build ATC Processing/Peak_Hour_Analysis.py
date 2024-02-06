import pandas as pd
import File_directories
import Create_DBs
from typing import List
import os
import glob
import re
import matplotlib.pyplot as plt
import seaborn as sns

"""
Python file used to produce peak hour analysis using 2022 or 2023 ATC data. This has only used 15m interval data with at least 5.5 days worth of data 
(defined using the sample size pivot table and dividing by 96 as an approximation to the number of days). 

This was also done using sites with at least 7.5 days worth of data as a sensitivity test, which yielded exactly the same peak hours.

Peak hours are based on total flow and an average flow profile plot for each site along with that for the sum of flows across all sites are produced.
"""


def filter_ATC_By_Sample_Size(input_file: str, site_filter: str, output_dir: str, sample_size: int):
    """
    Filters a database from folder 4_2 by sample size using a predefined csv containing a list of required sites.
    The required sites are found by using the sample size pivot tables output by the outlier filter functions in
    Data_Processing&Filtering.py, dividing the number of entried by 96 (number of 15m intervals in a day) and
    filtering the result.

    Note the value used on the filter was 5.5 rather than 6 to account for data lost when using the outlier filter.
    
    The resulting database should go into folder 4_3
    """

    #read input data
    data = pd.read_csv(input_file)
    #read csv containing list of required sites
    site_filter_df = pd.read_csv(site_filter)
    #define site - Source field to separate Severnside and HCC data
    data["site - Source"] = data["site"].astype(str) + " - " + data["Source"]

    #use .merge to apply the site filter
    filtered_data = pd.merge(data, site_filter_df, how = "inner", on = "site - Source")
    print(input_file)

    #define section of input file name to be kept for use on output file name
    file_name = re.findall('\\\\\S+?\.',input_file)[0][:-1]
    print(file_name)

    #output filtered db
    filtered_data.to_csv(f"{output_dir}\\{file_name}_{sample_size}.csv", index = False)

def peak_hour_analysis(input_db: str, output_dir: str, year: str, output_filename: str, sample_size: str):
    """
    Performs peak hour analysis on the input file. Results all output into the output directory
    The steps are:

    1. Calculate average flow by site, direction and time
    2. sum across the directions to find average two way flow and output as a separate csv file
    3. for each site, calculate rolling hour total flows (no issue arises with data loss because average has already been taken)
    4. define separate dataframes for the site corresponding to AM and PM peak periods, record the maximum flow and corresponding
       time for each period, before appending the data for the specific site to a dataframe containing the peak hours for all sites
    5. create plot for each site
    6. sum the average two way 15m flow across all sites and calculate rolling hour flows
    7. output the result and plot the flow profile

    The resulting outputs are listed below:
    1. Two way average flows by site in 15m intervals
    2. A record of AM and PM peak hours with their flows by site
    3. Flow profiles for each site
    4. 24 hour average rolling hour flows summed across all sites, along with a plot
    """
    #read input data
    Data = pd.read_csv(input_db)

    #store name of input db and change working directory to output dir for plots
    input_db_name = re.findall("ATC\S+.", input_db)[-1][:-1]
    print(input_db_name)
    os.chdir(output_dir)

    #define time column to calcuulate average flows
    Data['time'] = pd.to_datetime(Data['date'],dayfirst = True).dt.time

    #calculate average flows by site and direction before summing across directions for 2 way average flows by site
    Data_avg = Data.groupby(['site - Source','direction','time']).mean()
    Data_Two_Way = Data_avg.groupby(['site - Source','time']).sum().drop(columns = ['year'])
    Data_Two_Way = Data_Two_Way.reset_index()

    #output 2 way average flows
    Data_Two_Way.to_csv(f"{output_dir}\\{output_filename}_Two_Way_Avg_Flows.csv", index = False)


    hours = Data_Two_Way['time'].unique()

    #define dataframe to hold peak hours and peak hour flows by site
    peak_hour_df = pd.DataFrame(columns = ['site - Source', 'AM_peak', 'AM_peak_flow','PM_peak','PM_peak_flow'])

    #loop through sites
    for site in Data_Two_Way['site - Source'].unique():
        #find two way average flow for particular site
        df_site = Data_Two_Way.loc[Data_Two_Way["site - Source"] == site]
        #calculate rolling hour total flow
        df_site['Hourly_Flow'] =  df_site['flow_total'].rolling(4).sum().shift(periods = -3)

        #create new datetime object column in order to define peak period dataframes
        df_site['dummy_datetime'] = "01/02/2022 " + df_site['time'].astype(str)
        df_site['dummy_datetime'] = pd.to_datetime(df_site['dummy_datetime'],dayfirst = True)

        #set index to dummy datetime to create peak period dataframes
        df_site = df_site.set_index('dummy_datetime')
        peak_hour_site_df = pd.DataFrame()
        AM_site_df = df_site.between_time("07:00:00","10:00:00")
        PM_site_df = df_site.between_time("16:00:00","19:00:00")
        
        #find maximum hourly flow in AM and PM peak periods and record the value along with corresponding time
        AM_site_peak = 0

        for x in range(len(AM_site_df['time'])):
            if AM_site_df.iloc[x,9] > AM_site_peak:
                AM_site_peak = AM_site_df.iloc[x,9]
                AM_peak_hour = AM_site_df['time'][x]
        
        PM_site_peak = 0

        for x in range(len(PM_site_df['time'])):
            if PM_site_df.iloc[x,9] > PM_site_peak:
                PM_site_peak = PM_site_df.iloc[x,9]
                PM_peak_hour = PM_site_df['time'][x]

        #store recorded data in dictionary and convert to dataframe
        site_data = {"site - Source": [site], "AM_peak": [AM_peak_hour], "AM_peak_hour_flow": [AM_site_peak], "PM_peak": [PM_peak_hour], "PM_peak_hour_flow": [PM_site_peak]}
        site_data = pd.DataFrame(site_data)
        
        #define max flow for specific site for use in plot
        maxflow_site = df_site["Hourly_Flow"].max()
        #convert time column to string series for use in axis
        df_site['time'] = df_site['time'].astype(dtype = str)
        df_site = df_site.reset_index()
        g= sns.relplot(x="time",y="Hourly_Flow",kind="line",data=df_site,height=8.27, aspect=25/8,sort=False)
        
        #append site specific peak hour info to peak hour df containing all sites
        peak_hour_df = pd.concat([peak_hour_df,site_data])

        #save flow profile plot for specific site
        g.set(ylim=(0,maxflow_site))
        g.set(ylabel="Flow (veh/h)")
        z = "Site "+str(site)+f" ATC average survey flow {year}"
        plt.title(z)
        plt.savefig(z)
        plt.close('all')

    print("saved all site figures")
    #output dataframe showing peak hours by site with peak flows
    peak_hour_df.to_csv(f"{output_dir}\\{output_filename}_Peak_hours_by_site.csv")

    #calculate average rolling hour flows summed across all site, plot the resulting flow profile
    total_hourly_flow_24h = Data_Two_Way.groupby(["time"]).sum().reset_index()
    total_hourly_flow_24h['Hourly_Flow'] =  total_hourly_flow_24h['flow_total'].rolling(4).sum().shift(periods = -3)
    total_hourly_flow_24h.to_csv(f"{output_filename}_Total_Flow_24h_PH_Analysis_{sample_size}_days.csv")
    maxflow = total_hourly_flow_24h["Hourly_Flow"].max()
    total_hourly_flow_24h['time'] = total_hourly_flow_24h['time'].astype(dtype = str)
    g=sns.relplot(x="time",y="Hourly_Flow",kind="line",data=total_hourly_flow_24h,height=8.27, aspect=25/8,sort=False)
    g.set(ylim=(0,maxflow))
    g.set(ylabel="Flow (veh/h)")
    print("saving overall curve")
    
    z = f"{output_filename} Total Average Hourly Flow"
    plt.title(z)       
    plt.savefig(z)



if __name__ == "__main__":
    os.chdir(os.getcwd())
    #filter_ATC_By_Sample_Size(f"{File_directories.ATC_OUTLIER_FILTERED}\\2023\\ATC_2023_15m_appended_neutral_mon-thurs_filtered_outliers_database.csv",File_directories.SAMPLE_FILTER_8,File_directories.PRE_PEAK_HOUR_DBS,8)
    filter_ATC_By_Sample_Size(f"{File_directories.ATC_OUTLIER_FILTERED}\\2023\\ATC_2023_15m_appended_neutral_mon-thurs_filtered_outliers_database.csv",File_directories.SAMPLE_FILTER_6,File_directories.PRE_PEAK_HOUR_DBS,6)
    

    peak_hour_analysis(File_directories.PEAK_HOUR_INPUT_2023_6DAYS,File_directories.PEAK_HOUR_2023,"2023", "2023_ATC_HCC_Severnside_6Days", "6")
    #peak_hour_analysis(File_directories.PEAK_HOUR_INPUT_2023_8DAYS,File_directories.PEAK_HOUR_2023,"2023", "2023_ATC_HCC_Severnside_8Days", "8")