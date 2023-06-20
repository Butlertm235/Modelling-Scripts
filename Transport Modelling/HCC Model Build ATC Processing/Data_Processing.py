import pandas as pd
import File_directories
import Create_DBs
from typing import List
import os
import glob
import re
import matplotlib.pyplot as plt
import seaborn as sns

def Format_ATC_DBs():
    #2019 NC
    #Create_DBs.format_ATC_2019_NC_1(File_directories.ATC_2019_NC_1, File_directories.ATC_2019_HOURLY_DB)
    #Create_DBs.format_ATC_2019_NC_2(File_directories.ATC_2019_NC_2, File_directories.ATC_2019_HOURLY_DB)
    #Create_DBs.format_ATC_2019_NC_3(File_directories.ATC_2019_NC_3, File_directories.ATC_2019_15M_DB)

    #2019 C
    #Create_DBs.format_ATC_C_1_1(File_directories.ATC_2019_C_1_1, File_directories.ATC_2019_15M_DB, 2019)
    #Create_DBs.format_ATC_C_1_2(File_directories.ATC_2019_C_1_2, File_directories.ATC_2019_15M_DB, 2019)

    #2022 NC
    #Create_DBs.format_ATC_2022_NC_1(File_directories.ATC_2022_NC_1,File_directories.ATC_2022_HOURLY_DB)
    #Create_DBs.format_ATC_2022_NC_2(File_directories.ATC_2022_NC_2,File_directories.ATC_2022_HOURLY_DB)
    #Create_DBs.format_ATC_2022_NC_3(File_directories.ATC_2022_NC_3,File_directories.ATC_2022_HOURLY_DB)
    #Create_DBs.format_ATC_2022_NC_4(File_directories.ATC_2022_NC_4,File_directories.ATC_2022_15M_DB)

    #2022 C
    #Create_DBs.format_ATC_C_1_1(File_directories.ATC_2022_C_1_1, File_directories.ATC_2022_15M_DB, 2022)
    #Create_DBs.format_ATC_C_1_2(File_directories.ATC_2022_C_1_2, File_directories.ATC_2022_15M_DB, 2022)
    #Create_DBs.format_ATC_2022_C_2_1(File_directories.ATC_2022_C_2_1, File_directories.ATC_2022_15M_DB, File_directories.DISCARDED_DATA)
    #Create_DBs.format_ATC_2022_C_2_2(File_directories.ATC_2022_C_2_2, File_directories.ATC_2022_15M_DB, File_directories.DISCARDED_DATA)

    #2023 NC
    #Create_DBs.format_ATC_2023_HCC_NC_1(File_directories.ATC_2023_HCC_NC_1, File_directories.ATC_2023_HOURLY_DB,File_directories.DISCARDED_DATA)
    #Create_DBs.format_ATC_2023_HCC_NC_2(File_directories.ATC_2023_HCC_NC_2, File_directories.ATC_2023_15M_DB,File_directories.DISCARDED_DATA)

    #2023_C
    Create_DBs.format_ATC_2023_HCC_C_1(File_directories.ATC_2023_HCC_C_1, File_directories.ATC_2023_15M_DB,File_directories.DISCARDED_DATA)
    #Create_DBs.format_ATC_2023_HCC_C_2(File_directories.ATC_2023_HCC_C_2, File_directories.ATC_2023_15M_DB,File_directories.DISCARDED_DATA)
    #Create_DBs.format_ATC_2023_SEVERNSIDE(File_directories.ATC_2023_SEVERNSIDE_RAW,File_directories.ATC_2023_15M_DB)


def append_db_groups(input_dir: str, output_dir: str, output_db_name: str):

    """
    This function will take a directory containing formatted DBs to be appended, and output to a specified directory 
    with a specified file name.
    Different combinations of formatted DB directories should be used for different purposes. i.e. 15m kept together to be 
    converted into hourly after outlier filtering.
    """

    DBs_to_append = []
    
    os.chdir(input_dir)
    files = glob.glob("*.csv")
    for file in files:

        print("Reading ", file, ", and adding to the list of appended DBs")

        file_df = pd.read_csv(file, skiprows = 0)
        print("DB List Type: ", type(DBs_to_append))
        DBs_to_append.insert(len(DBs_to_append), file_df)
        print(DBs_to_append)

    print("Total DBs appended = ", len(DBs_to_append))

    Appended_DB = pd.concat(DBs_to_append)

    Appended_DB.to_csv(f"{output_dir}\\{output_db_name}.csv")

def append_ATC_dbs():
    """Appends all Formatted ATC DBs, keeping them separated by year and time interval"""
    for DB in File_directories.ATC_FORMATTED_DBS.items():
        dict = DB[1]
        print(dict)
        append_db_groups(dict['directory'],File_directories.ATC_APPENDED,f"ATC_{dict['year']}_{dict['interval']}_appended")


def filter_db_mon_thurs(input_dir: str, day_filter_csv: str, output_dir: str):
    """
    This will take a directory containing appended databases and, remove duplicates before filtering the data to include only neutral days between monday and thursday
    day_filter_csv must be passed as an entire directory
    """
    day_filter = pd.read_csv(day_filter_csv)

    os.chdir(input_dir)
    files = glob.glob("*.csv")

    for file in files:
        
        print("Reading ", file, ", and filtering")

        data = pd.read_csv(file, skiprows = 0)


        data.drop_duplicates(subset = ['direction', 'site', 'date'], keep = "first", inplace = True)

        data['Date'] = pd.to_datetime(data['date'], dayfirst = True).dt.date

        data = pd.merge(data, day_filter, on = "Date", how = "inner")
        file = re.findall('^.+\.', file)
        print(file)
        data.to_csv(f"{output_dir}_{file}_neutral_mon-thurs.csv")

def outlier_filter(input_DB: str, output_dir: str):
    """Filters one file for outliers after removing duplicated, working directory must be changed to the input file's directory before running"""
    
    
    input_data = pd.read_csv(input_DB)
    ######REMOVE DUPLICATES#############

    input_data['year'] = pd.to_datetime(input_data['date'],dayfirst = True).dt.year
    input_data['month'] = pd.to_datetime(input_data['date'],dayfirst = True).dt.month
    input_data['day'] = pd.to_datetime(input_data['date'],dayfirst = True).dt.day
    input_data['time'] = pd.to_datetime(input_data['date'],dayfirst = True).dt.time


    not_null_sample_size = input_data.groupby(["site","source","direction","year","month"]).count()
    not_null_sample_size_pivot = not_null_sample_size.pivot_table(index=["site","source","direction"],columns =["year","month"],values = "day")
    not_null_values = len(Data.index)

    #Definition of the z score
    zscore = lambda x: abs(x-x.mean())/x.std()

    #Calculation of the z score for the hour group. Filtering of values above the threshold defined by the user.
    input_data["zscore_time"] = input_data.groupby(["site","source","direction","time"])["flow_total"].transform(zscore)
    input_data = input_data[input_data["zscore_time"]< 4]
    z_time_values = len(input_data.index)
    print("Number of records removed with z_time: {}".format(not_null_values-z_time_values))

    #Calculation of the z score for the month group. Filtering of values above the threshold defined by the user.
    input_data["zscore_month"] = input_data.groupby(["site","source","direction","time","year","month"])["flow_total"].transform(zscore)
    input_data = input_data[input_data["zscore_month"]< 2]
    z_month_values = len(input_data.index)
    print("Number of records removed with z_month: {}".format(z_time_values-z_month_values))

    #Remove sets that don't have at least 3 records.
    record_filter = lambda x: x["flow_total"].count() >= 3    
    input_data = input_data.groupby(["site","source","direction","time","year","month"]).filter(record_filter)
    record_filter_values = len(input_data.index)
    print("Number of records removed with record filter: {}".format(z_month_values-record_filter_values))

    print("Total number of removed records: {} ({:.2f}%)".format(not_null_values-record_filter_values,(not_null_values-record_filter_values)/not_null_values*100))

    #Generate matrix with final records and data coverage by comparing pre and post outlier filtering
    final_sample_size = input_data.groupby(["site","source","direction","year","month"]).count()
    final_sample_size_pivot = final_sample_size.pivot_table(index=["site","source","direction"],columns =["year","month"],values = "day")                    
    final_data_coverage = final_sample_size_pivot/not_null_sample_size_pivot

    input_DB_name = input_DB[:-3]
    #Export the dataframe to a .csv file
    export_csv = input_data.to_csv(f"{output_dir}\\{input_DB_name}_filtered_outliers_database.csv")
    export_csv = final_sample_size_pivot.to_csv(f"{output_dir}\\{input_DB_name}_final_sample_size_pivot.csv")
    export_csv = final_data_coverage.to_csv(f"{output_dir}\\{input_DB_name}_Outlier_filter_percentage.csv")


def filter_ATC_outliers(input_dir, output_dir):
    """ Applies the outlier filter to all databases in a given directory, outputting results to another given directory"""
    os.chdir(input_dir)

    files = glob.glob("*.csv")

    for file in files:
        outlier_filter(file,output_dir)

def peak_hour_analysis(input_db: str, output_dir: str, year: str):
    """ Performs peak hour analysis on the input file. Results all output into the output directory"""
    Data = pd.read_csv(input_db)

    input_db_name = re.finall("^\+S.", input_db)[1:-1]
    
    os.chdir(output_dir)

    Data = Data.drop(columns = ['zscore_time', 'zscore_month','Unnamed: 0', "Unnamed: 0.1"])

    Data['time'] = pd.to_datetime(Data['date'],dayfirst = True).dt.time

    Data_avg = Data.groupby(['site','direction','source','time']).mean()
    Data_Two_Way = Data_avg.groupby(['site','source','time']).sum().drop(columns = ['year'])

    Data_Two_Way.to_csv(f"{output_dir}\\{input_db_name}_Two_Way_Avg_Flows.csv")

    Data_Two_Way = Data_Two_Way.reset_index()
    hours = Data_Two_Way['time'].unique()

    peak_hour_df = pd.DataFrame(columns = ['site', 'AM_peak', 'AM_peak_flow','PM_peak','PM_peak_flow'])

    for site in Data_Two_Way['site'].unique():
        df_site = Data_Two_Way.loc[Data_Two_Way["site"] == site]
        df_site['Hourly_Flow'] =  df_site['flow_total'].rolling(4).sum().shift(periods = -3)

        df_site['dummy_datetime'] = "01/02/2022 " + df_site['time'].astype(str)
        
        df_site['dummy_datetime'] = pd.to_datetime(df_site['dummy_datetime'],dayfirst = True)

        
        df_site = df_site.set_index('dummy_datetime')
        peak_hour_site_df = pd.DataFrame()
        
        
        AM_site_df = df_site.between_time("07:00:00","10:00:00")
        PM_site_df = df_site.between_time("16:00:00","19:00:00")

        AM_site_peak = 0

        for x in range(len(AM_site_df['time'])):
            if AM_site_df.iloc[x,10] > AM_site_peak:
                #print("Test")
                AM_site_peak = AM_site_df.iloc[x,10]
                AM_peak_hour = AM_site_df['time'][x]
                #print(AM_site_peak)
        
        PM_site_peak = 0
        for x in range(len(PM_site_df['time'])):
            if PM_site_df.iloc[x,10] > PM_site_peak:
                PM_site_peak = PM_site_df.iloc[x,10]
                PM_peak_hour = PM_site_df['time'][x]

        #peak_hour_site_df['site'] = site
        #peak_hour_site_df['AM_peak'] = AM_peak_hour
        #peak_hour_site_df['AM_peak_flow'] = AM_site_peak
        

        #peak_hour_site_df['PM_peak'] = PM_peak_hour
        #peak_hour_site_df['PM_peak_flow'] = PM_site_peak

        site_data = {"site": [site], "AM_peak": [AM_peak_hour], "AM_peak_hour_flow": [AM_site_peak], "PM_peak": [PM_peak_hour], "PM_peak_hour_flow": [PM_site_peak]}
        site_data = pd.DataFrame(site_data)
        

        maxflow_site = df_site["Hourly_Flow"].max()
        df_site['time'] = df_site['time'].astype(dtype = str)
        g= sns.relplot(x="time",y="Hourly_Flow",kind="line",data=df_site,height=8.27, aspect=25/8,sort=False)
        
        peak_hour_df = pd.concat([peak_hour_df,site_data])

        g.set(ylim=(0,maxflow_site))
        g.set(ylabel="Flow (veh/h)")
        #sns.catplot(x="month",y="flow",data=df,kind="box", whis=1.5)
        #plt.show()
        z = "Site "+str(site)+f" ATC average survey flow {year}"
        plt.title(z)       
        plt.savefig(z)

    peak_hour_df.to_csv(f"{output_dir}\\{input_db_name}_Peak_hours_by_site.csv")

    total_hourly_flow_24h = Data_Two_Way.groupby(["time"]).sum().reset_index()
    total_hourly_flow_24h['Hourly_Flow'] =  total_hourly_flow_24h['flow_total'].rolling(4).sum().shift(periods = -3)
    total_hourly_flow_24h.to_csv("Total_Flow_24h_PH_Analysis.csv")
    maxflow = total_hourly_flow_24h["Hourly_Flow"].max()
    total_hourly_flow_24h['time'] = total_hourly_flow_24h['time'].astype(dtype = str)
    g=sns.relplot(x="time",y="Hourly_Flow",kind="line",data=total_hourly_flow_24h,height=8.27, aspect=25/8,sort=False)
    g.set(ylim=(0,maxflow))
    g.set(ylabel="Flow (veh/h)")
    #sns.catplot(x="month",y="flow",data=df,kind="box", whis=1.5)
    #plt.show()
    z = f"{input_db_name} Total Average Hourly Flow"
    plt.title(z)       
    plt.savefig(z)





if __name__ == "__main__":
    Format_ATC_DBs()
    #append_ATC_dbs()

    #filter_db_mon_thurs(File_directories.ATC_APPENDED, File_directories.DATE_FILTER_MON_THURS_NEUTRAL,File_directories.ATC_MON_THURS_NEUTRAL)

    #filter_ATC_outliers(File_directories.ATC_MON_THURS_NEUTRAL,File_directories.ATC_OUTLIER_FILTERED)

