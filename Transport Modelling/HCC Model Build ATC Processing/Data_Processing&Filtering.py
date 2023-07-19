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
    #Create_DBs.format_ATC_C_1_1(File_directories.ATC_2019_C_1_1, File_directories.ATC_2019_15M_DB, 2019, File_directories.DISCARDED_DATA)
    #Create_DBs.format_ATC_C_1_2(File_directories.ATC_2019_C_1_2, File_directories.ATC_2019_15M_DB, 2019)

    #2022 NC
    #Create_DBs.format_ATC_2022_NC_1(File_directories.ATC_2022_NC_1,File_directories.ATC_2022_HOURLY_DB)
    #Create_DBs.format_ATC_2022_NC_2(File_directories.ATC_2022_NC_2,File_directories.ATC_2022_HOURLY_DB)
    #Create_DBs.format_ATC_2022_NC_3(File_directories.ATC_2022_NC_3,File_directories.ATC_2022_HOURLY_DB)
    #Create_DBs.format_ATC_2022_NC_4(File_directories.ATC_2022_NC_4,File_directories.ATC_2022_15M_DB)

    #2022 C
    #Create_DBs.format_ATC_C_1_1(File_directories.ATC_2022_C_1_1, File_directories.ATC_2022_15M_DB, 2022, File_directories.DISCARDED_DATA)
    #Create_DBs.format_ATC_C_1_2(File_directories.ATC_2022_C_1_2, File_directories.ATC_2022_15M_DB, 2022)
    #Create_DBs.format_ATC_2022_C_2_1(File_directories.ATC_2022_C_2_1, File_directories.ATC_2022_15M_DB, File_directories.DISCARDED_DATA)
    Create_DBs.format_ATC_2022_C_2_2(File_directories.ATC_2022_C_2_2, File_directories.ATC_2022_15M_DB, File_directories.DISCARDED_DATA)

    #2023 NC
    #Create_DBs.format_ATC_2023_HCC_NC_1(File_directories.ATC_2023_HCC_NC_1, File_directories.ATC_2023_HOURLY_DB,File_directories.DISCARDED_DATA)
    #Create_DBs.format_ATC_2023_HCC_NC_2(File_directories.ATC_2023_HCC_NC_2, File_directories.ATC_2023_15M_DB,File_directories.DISCARDED_DATA)

    #2023_C
    #Create_DBs.format_ATC_2023_HCC_C_1(File_directories.ATC_2023_HCC_C_1, File_directories.ATC_2023_15M_DB,File_directories.DISCARDED_DATA)
    #Create_DBs.format_ATC_2023_HCC_C_2(File_directories.ATC_2023_HCC_C_2, File_directories.ATC_2023_15M_DB,File_directories.DISCARDED_DATA)
    #Create_DBs.format_ATC_2023_SEVERNSIDE(File_directories.ATC_2023_SEVERNSIDE_RAW,File_directories.ATC_2023_15M_DB)
    #Create_DBs.ATC_HCC_C_1_2(File_directories.ATC_2019_C_1_2, File_directories.ATC_2019_15M_DB,File_directories.DISCARDED_DATA, 2019)
    #Create_DBs.ATC_HCC_C_1_2(File_directories.ATC_2022_C_1_2, File_directories.ATC_2022_15M_DB, File_directories.DISCARDED_DATA, 2022)
    #Create_DBs.ATC_HCC_C_1_2(File_directories.ATC_2023_HCC_C_1, File_directories.ATC_2023_15M_DB, File_directories.DISCARDED_DATA, 2023)


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

    Appended_DB = pd.concat(DBs_to_append, ignore_index = True)

    Appended_DB.to_csv(f"{output_dir}\\{output_db_name}.csv", index = False)

def append_ATC_dbs():
    """Appends all Formatted ATC DBs, keeping them separated by year and time interval"""
    for DB in File_directories.ATC_FORMATTED_DBS.items():
        dict = DB[1]
        print(dict)
        append_db_groups(dict['directory'],File_directories.ATC_APPENDED,f"{str(dict['year'])}\\ATC_{dict['year']}_{dict['interval']}_appended")


def filter_db_mon_thurs(input_dir: str, day_filter_csv: str, output_dir: str):
    """
    This will take a directory containing appended databases and, remove duplicates before filtering the data to include only neutral days between monday and thursday
    day_filter_csv must be passed as an entire directory
    """
    day_filter = pd.read_csv(day_filter_csv)

    os.chdir(input_dir)
    files = glob.glob("*.csv")

    day_filter['Date'] = pd.to_datetime(day_filter["Date"], dayfirst = True).dt.date



    for file in files:
        
        print("Reading ", file, ", and filtering")

        data = pd.read_csv(file, skiprows = 0)


        data.drop_duplicates(subset = ['direction', 'site', 'date'], keep = "first", inplace = True)

        data['Date'] = pd.to_datetime(data['date'], dayfirst = True).dt.date


        data = pd.merge(data, day_filter, on = "Date", how = "inner")
        #file = re.findall('^.+\.', file)
        #print(file)
        
        data = data.drop(columns = ["Date"])

        data.to_csv(f"{output_dir}\\{file[:-4]}_neutral_mon-thurs.csv", index = False)

def filter_ATC_DBs_Mon_Thurs(input_dir: str, day_filter_csv: str, output_dir: str):
    for year in ["2019", "2022"]: #, "2023"
        filter_db_mon_thurs(f"{input_dir}\\{year}\\", day_filter_csv, f"{output_dir}\\{year}\\")


def outlier_filter(input_DB: str, output_dir: str):
    """Filters one file for outliers after removing duplicated, working directory must be changed to the input file's directory before running"""
    
    
    input_data = pd.read_csv(input_DB)
    ######REMOVE DUPLICATES#############
    input_data.drop_duplicates(subset = ["site", "date", "direction", "Source"], keep = "first")
    
    input_data['year'] = pd.to_datetime(input_data['date'],dayfirst = True).dt.year
    input_data['month'] = pd.to_datetime(input_data['date'],dayfirst = True).dt.month
    input_data['day'] = pd.to_datetime(input_data['date'],dayfirst = True).dt.day
    input_data['time'] = pd.to_datetime(input_data['date'],dayfirst = True).dt.time

    not_null_sample_size = input_data.groupby(["site","Source","direction","year","month"]).count()
    not_null_sample_size_pivot = not_null_sample_size.pivot_table(index=["site","Source","direction"],columns =["year","month"],values = "day")
    not_null_values = len(input_data.index)

    #Definition of the z score
    zscore = lambda x: abs(x-x.mean())/x.std()

    #Calculation of the z score for the hour group. Filtering of values above the threshold defined by the user.
    input_data["zscore_time"] = input_data.groupby(["site","Source","direction","time"])["flow_total"].transform(zscore)
    input_data = input_data[input_data["zscore_time"]< 4]
    z_time_values = len(input_data.index)
    print("Number of records removed with z_time: {}".format(not_null_values-z_time_values))

    #Calculation of the z score for the month group. Filtering of values above the threshold defined by the user.
    input_data["zscore_month"] = input_data.groupby(["site","Source","direction","time","year","month"])["flow_total"].transform(zscore)
    input_data = input_data[input_data["zscore_month"]< 2]
    z_month_values = len(input_data.index)
    print("Number of records removed with z_month: {}".format(z_time_values-z_month_values))

    #Remove sets that don't have at least 3 records.
    record_filter = lambda x: x["flow_total"].count() >= 3    
    input_data = input_data.groupby(["site","Source","direction","time","year","month"]).filter(record_filter)
    record_filter_values = len(input_data.index)
    print("Number of records removed with record filter: {}".format(z_month_values-record_filter_values))

    print("Total number of removed records: {} ({:.2f}%)".format(not_null_values-record_filter_values,(not_null_values-record_filter_values)/not_null_values*100))

    #Generate matrix with final records and data coverage by comparing pre and post outlier filtering
    final_sample_size = input_data.groupby(["site","Source","direction","year","month"]).count()
    final_sample_size_pivot = final_sample_size.pivot_table(index=["site","Source","direction"],columns =["year","month"],values = "day")                    
    final_data_coverage = final_sample_size_pivot/not_null_sample_size_pivot

    input_DB_name = input_DB[:-3]
    #Export the dataframe to a .csv file
    input_data.to_csv(f"{output_dir}\\{input_DB_name}_filtered_outliers_database.csv", index = False)
    final_sample_size_pivot.to_csv(f"{output_dir}\\{input_DB_name}_final_sample_size_pivot.csv")
    final_data_coverage.to_csv(f"{output_dir}\\{input_DB_name}_Outlier_filter_percentage.csv")


def outlier_filter_2023(input_DB: str, output_dir: str):
    """Filters one 2023 file for outliers, working directory must be changed to the input file's directory before running"""
    
    input_data = pd.read_csv(input_DB)
    ######REMOVE DUPLICATES#############
    
    input_data.drop_duplicates(subset = ["site", "date", "direction", "Source"], keep = "first")
    
    input_data['year'] = pd.to_datetime(input_data['date'],dayfirst = True).dt.year
    input_data['month'] = pd.to_datetime(input_data['date'],dayfirst = True).dt.month
    input_data['day'] = pd.to_datetime(input_data['date'],dayfirst = True).dt.day
    input_data['time'] = pd.to_datetime(input_data['date'],dayfirst = True).dt.time

    not_null_sample_size = input_data.groupby(["site","Source","direction","year","month"]).count()
    not_null_sample_size_pivot = not_null_sample_size.pivot_table(index=["site","Source","direction"],columns =["year","month"],values = "day")
    not_null_values = len(input_data.index)

    #Definition of the z score
    zscore = lambda x: abs(x-x.mean())/x.std()

    #Calculation of the z score for the hour group. Filtering of values above the threshold defined by the user.
    input_data["zscore_time"] = input_data.groupby(["site","Source","direction","time"])["flow_total"].transform(zscore)
    
    input_data = input_data[input_data["zscore_time"]< 1.5]
    
    z_time_values = len(input_data.index)
    print("Number of records removed with z_time: {}".format(not_null_values-z_time_values))

    #Remove sets that don't have at least 3 records.
    record_filter = lambda x: x["flow_total"].count() >= 3    
    print(input_data.loc[input_data["site"] == 104])
    input_data = input_data.groupby(["site","Source","direction","time","year"]).filter(record_filter)
    print(input_data.loc[input_data["site"] == 104])
    record_filter_values = len(input_data.index)
    print("Number of records removed with record filter: {}".format(z_time_values-record_filter_values))

    print("Total number of removed records: {} ({:.2f}%)".format(not_null_values-record_filter_values,(not_null_values-record_filter_values)/not_null_values*100))

    #Generate matrix with final records and data coverage by comparing pre and post outlier filtering
    final_sample_size = input_data.groupby(["site","Source","direction","year","month"]).count()
    final_sample_size_pivot = final_sample_size.pivot_table(index=["site","Source","direction"],columns =["year","month"],values = "day")                    
    final_data_coverage = final_sample_size_pivot/not_null_sample_size_pivot

    input_DB_name = input_DB[:-3]
    #Export the dataframe to a .csv file
    input_data.to_csv(f"{output_dir}\\{input_DB_name}_filtered_outliers_database_Test.csv", index = False)
    final_sample_size_pivot.to_csv(f"{output_dir}\\{input_DB_name}_final_sample_size_pivot_Test.csv")
    final_data_coverage.to_csv(f"{output_dir}\\{input_DB_name}_Outlier_filter_percentage_Test.csv")


def filter_ATC_outliers(input_dir, output_dir):
    """ Applies the outlier filter to all databases in a given neutral day directory, outputting results to another given directory"""
    
    for year in ["2019", "2022", "2023"]: #
        os.chdir(f"{input_dir}\\{year}")
        files = glob.glob("*.csv")

        for file in files:

            print(f"Filtering File: {file}")
            if year == "2023":
                outlier_filter_2023(file, f"{output_dir}\\{year}")
            else:
                outlier_filter(file,f"{output_dir}\\{year}")
    






if __name__ == "__main__":
    os.chdir(os.getcwd())
    Format_ATC_DBs()

    #Create_DBs.format_ATC_2023_SEVERNSIDE("C:\\Users\\UKHRM004\\Documents\\HCC Model Update\\For Github\\1 - Raw Data\ATC\\2023\\Severnside\\Test","C:\\Users\\UKHRM004\\Documents\\HCC Model Update\\For Github\\1 - Raw Data\\ATC\\2023\\Severnside\\Test")


    #data_2023_15m = pd.read_csv(f"{File_directories.ATC_APPENDED}\\2023\\ATC_2023_15m_appended.csv")
    #data_2023_15m["site-Source"] = data_2023_15m["site"].astype(str) + " - " + data_2023_15m["Source"]
    #print(len(data_2023_15m["site-Source"].unique()))
    append_ATC_dbs()
    
    filter_ATC_DBs_Mon_Thurs(File_directories.ATC_APPENDED, File_directories.DATE_FILTER_MON_THURS_NEUTRAL, File_directories.ATC_MON_THURS_NEUTRAL)

    filter_ATC_outliers(File_directories.ATC_MON_THURS_NEUTRAL,File_directories.ATC_OUTLIER_FILTERED)
    