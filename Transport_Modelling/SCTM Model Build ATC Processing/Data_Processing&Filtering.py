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
import util

    

def append_db_groups(input_dir: str, output_dir: str, output_db_name: str):

    """
    This function will take a directory containing formatted DBs to be appended, and output the appended database 
    to a specified directory with a specified file name.
    
    It is intended to be used with the dictionary ATC_FORMATTED_DBS defined in File_directories.py.
    This dictionary has a correspondence for each intended output file with their respective input directory,
    along with the corresponding year and time inteval which is used in the output file name.
    
    Different combinations of formatted DB directories should be used for different purposes. i.e. 15m kept together to be 
    converted into hourly after outlier filtering, or to be used in peak hour analysis
    """

    #define list of dataframes to append
    DBs_to_append = []
    os.chdir(input_dir)
    files = glob.glob("*.csv")

    #loop throught files in input directory
    for file in files:

        print("Reading ", file, ", and adding to the list of appended DBs")

        #read in file and add to list of dataframes to append
        file_df = pd.read_csv(file, skiprows = 0)
        DBs_to_append.insert(len(DBs_to_append), file_df)
        print(DBs_to_append)

    #print count of files read
    print("Total DBs appended = ", len(DBs_to_append))

    #concatenate all dataframes in list and output
    Appended_DB = pd.concat(DBs_to_append, ignore_index = True)
    Appended_DB.to_csv(f"{output_dir}\\{output_db_name}.csv", index = False)

def append_ATC_dbs():
    """Appends all Formatted ATC DBs, using the dictionary defined in File_directories.py for the naming convention,
    keeping the resulting databases separated by year and time interval"""

    #loop through entries in dictionary corresponding to each year / time interval
    for DB in File_directories.ATC_FORMATTED_DBS.items():
        #define dictionary variable containing input directory, year, and interval info for each of the required databases
        dict = DB[1]
        print(dict)

        #append all files within the given input directory, using the year and time interval in the file name
        append_db_groups(dict['directory'],File_directories.ATC_APPENDED,f"{str(dict['year'])}\\ATC_{dict['year']}_{dict['interval']}_appended")


def filter_db_mon_thurs(input_dir: str, day_filter_csv: str, output_dir: str):
    """
    This will take a directory containing appended databases and, remove duplicates before filtering the data to include only neutral days 
    between monday and thursday day_filter_csv must be passed as an entire directory, defined in File_directories.py
    
    Note: 15m and hourly dataframes are kept separate so further duplicate removal is needed later
    
    """
    #read date filter csv and convert dates to datetime objects
    day_filter = pd.read_csv(day_filter_csv)
    day_filter['Date'] = pd.to_datetime(day_filter["Date"], dayfirst = True).dt.date
    
    #define list of file names and loop through, reading the csv each time
    os.chdir(input_dir)
    files = glob.glob("*.csv")
    for file in files:
        print("Reading ", file, ", and filtering")
        data = pd.read_csv(file, skiprows = 0)

        #drop duplicates based on direction, site and datetime. 15m and hourly dataframes are kept separate so further duplicate removal needed later
        data.drop_duplicates(subset = ['direction', 'site', 'date'], keep = "first", inplace = True)
        #convert formatted dataframe column to datetime
        data['Date'] = pd.to_datetime(data['date'], dayfirst = True).dt.date

        #apply filter
        data = pd.merge(data, day_filter, on = "Date", how = "inner")

        #drop unneeded date column and output
        data = data.drop(columns = ["Date"])
        data.to_csv(f"{output_dir}\\{file[:-4]}_neutral_mon-thurs.csv", index = False)



def filter_ATC_DBs_Mon_Thurs(input_dir: str, day_filter_csv: str, output_dir: str):
    """filter databases for neutral days for the specified years"""
    for year in ["2019", "2022", "2023"]: #
        filter_db_mon_thurs(f"{input_dir}\\{year}\\", day_filter_csv, f"{output_dir}\\{year}\\")


def combine_15m_hourly(input_dir: str, year: str, output_dir: str):
    """combines 15m and hourly databases into one hourly database for one year"""
    os.chdir(f"{input_dir}\\{year}\\")
    data_15m = pd.read_csv(f"ATC_{year}_15m_appended_neutral_mon-thurs.csv")
    data_hourly = pd.read_csv(f"ATC_{year}_Hourly_appended_neutral_mon-thurs.csv")

    data_15m_converted = util.convert_15m_db_to_hourly(data_15m)


    ############### To do: Check function upon turning 15m hourly conversion into separate util function
    ###############
    ############### Maybe add next three lines to util function
    data_15m_converted['date'] = data_15m_converted['Date'].astype(str) + " " + data_15m_converted['hour'].astype(str)
    data_15m_converted['date'] = pd.to_datetime(data_15m_converted['date'], dayfirst = True)
    data_15m_converted = data_15m_converted.drop(columns = ["hour", "Date"])


    combined_df = pd.concat([data_15m_converted, data_hourly])
    combined_df.drop_duplicates(subset = ["site", "date", "direction", "Source"], keep = "first")

    combined_df.to_csv(f"{output_dir}\\ATC_{year}_Combined.csv", index = False)
    #data_15m_converted.to_csv(f"{output_dir}\\ATC_{year}_Hourly_conv_Test.csv", index = False)


def combine_all_neutral_day_dbs(input_dir: str, output_dir: str):
    """Combines all 15m databases to respective hourly database using hourly intervals"""
    for year in ["2019", "2022", "2023"]: #, "2023"
        combine_15m_hourly(input_dir, year, output_dir)


def outlier_filter(input_DB: str, output_dir: str):
    """
    Filters one file for outliers after removing duplicated data, working directory must be changed to the input file's directory before running
    This function is only used for filtering 2022 and 2019 datasets as there are different filters applied for 2023.
    Filters applied for 2019 and 2022:

    1. all data is filtered together with a z score threshold of +-4
    2. data for each month is filtered separately with a z score threshold of +-2
    3. if there are fewer than three records for a particular month for a given site, those records are discarded using the record filter.
       note that only the records for the month in question are discarded and not those for the entire site
    
    2023 data is filtered using a separate function (see below) which only includes the first step with a z score threshold of 2,
    along with the third step with no dependence on the month of the data entries

    The files created are:
    1. A pivot table of the number of entries for each site, broken down by month. This has been subsequently used to create an approximation for the number
    of full days for each month and site in excel by dividing by the number of 15m or hourly intervals in a day
    2. Another pivot table showing the proportion of data kept by site and month
    3. The filtered database
    """
    
    #read input data
    input_data = pd.read_csv(input_DB)
    #remove duplicates based on site, date, direction and source
    input_data.drop_duplicates(subset = ["site", "date", "direction", "Source"], keep = "first")
    
    #split datetime object in order to calculate relevant z-scores. year is separated in case the function is ever used to filter a database spanning multiple years
    input_data['year'] = pd.to_datetime(input_data['date'],dayfirst = True).dt.year
    input_data['month'] = pd.to_datetime(input_data['date'],dayfirst = True).dt.month
    input_data['day'] = pd.to_datetime(input_data['date'],dayfirst = True).dt.day
    input_data['time'] = pd.to_datetime(input_data['date'],dayfirst = True).dt.time

    #count datapoints for each month by site source and direction
    not_null_sample_size = input_data.groupby(["site","Source","direction","year","month"]).count()
    #create pivot table showing number of entries broken down by site, source, direction, month, year. Note this line does NOT count number of days as the previous line 
    #will count number of entries rather than number of days
    not_null_sample_size_pivot = not_null_sample_size.pivot_table(index=["site","Source","direction"],columns =["year","month"],values = "day")
    
    #store total number of values to calculate data loss
    not_null_values = len(input_data.index)

    #Definition of the z score
    zscore = lambda x: abs(x-x.mean())/x.std()

    #Calculation of the z score for the hour group. Filtering of values above the threshold defined by the user.
    input_data["zscore_time"] = input_data.groupby(["site","Source","direction","time"])["flow_total"].transform(zscore)
    input_data = input_data[input_data["zscore_time"]< 4]
    
    #store number of values left after first filter to calculate data loss
    z_time_values = len(input_data.index)
    print("Number of records removed with z_time: {}".format(not_null_values-z_time_values))

    #Calculation of the z score for the month group. Filtering of values above the threshold defined by the user.
    input_data["zscore_month"] = input_data.groupby(["site","Source","direction","time","year","month"])["flow_total"].transform(zscore)
    input_data = input_data[input_data["zscore_month"]< 2]
    
    #store number of values left after second filter to calculate data loss
    z_month_values = len(input_data.index)
    print("Number of records removed with z_month: {}".format(z_time_values-z_month_values))

    #Remove sets that don't have at least 3 records.
    record_filter = lambda x: x["flow_total"].count() >= 3    
    input_data = input_data.groupby(["site","Source","direction","time","year","month"]).filter(record_filter)
    
    #store number of values left after final filter to calclate data loss
    record_filter_values = len(input_data.index)
    print("Number of records removed with record filter: {}".format(z_month_values-record_filter_values))

    #print % data loss
    print("Total number of removed records: {} ({:.2f}%)".format(not_null_values-record_filter_values,(not_null_values-record_filter_values)/not_null_values*100))

    #Generate matrix with final records and data coverage by comparing pre and post outlier filtering
    final_sample_size = input_data.groupby(["site","Source","direction","year","month"]).count()
    final_sample_size_pivot = final_sample_size.pivot_table(index=["site","Source","direction"],columns =["year","month"],values = "day")                    
    final_data_coverage = final_sample_size_pivot/not_null_sample_size_pivot

    input_DB_name = input_DB[:-4]
    #Export the dataframe to a .csv file after removing unneeded columns
    input_data = input_data.drop(columns = ["month", "day", "zscore_time", "zscore_month"])
    input_data.to_csv(f"{output_dir}\\{input_DB_name}_filtered_outliers_database.csv", index = False)
    final_sample_size_pivot.to_csv(f"{output_dir}\\{input_DB_name}_final_sample_size_pivot.csv")
    final_data_coverage.to_csv(f"{output_dir}\\{input_DB_name}_Outlier_filter_percentage.csv")


def outlier_filter_2023(input_DB: str, output_dir: str):
    """Filters one 2023 file for outliers, working directory must be changed to the input file's directory before running
    
    1. all data is filtered together with a z score threshold of +-2
    2. if there are fewer than three records for a given site, those records are discarded using the record filter.
    
    The files created are:
    1. A pivot table of the number of entries for each site, broken down by month. This has been subsequently used to create an approximation for the number
    of full days for each month and site in excel by dividing by the number of 15m or hourly intervals in a day
    2. Another pivot table showing the proportion of data kept by site and month
    3. The filtered database
    """
    
    input_data = pd.read_csv(input_DB)
    
    #remove duplicates based on site, source, date and direction
    input_data.drop_duplicates(subset = ["site", "date", "direction", "Source"], keep = "first")
    
    #split datetime column to apply aggregation and filters
    input_data['year'] = pd.to_datetime(input_data['date'],dayfirst = True).dt.year
    input_data['month'] = pd.to_datetime(input_data['date'],dayfirst = True).dt.month
    input_data['day'] = pd.to_datetime(input_data['date'],dayfirst = True).dt.day
    input_data['time'] = pd.to_datetime(input_data['date'],dayfirst = True).dt.time

    #count datapoints for each month by site source and direction
    not_null_sample_size = input_data.groupby(["site","Source","direction","year","month"]).count()
    #create pivot table showing number of entries broken down by site, source, direction, month, year. Note this line does NOT count number of days as the previous line 
    #will count number of entries rather than number of days
    not_null_sample_size_pivot = not_null_sample_size.pivot_table(index=["site","Source","direction"],columns =["year","month"],values = "day")
    
    #store total number of values to calculate data loss
    not_null_values = len(input_data.index)

    #Definition of the z score
    zscore = lambda x: abs(x-x.mean())/x.std()

    #Calculation of the z score for the hour group. Filtering of values above the threshold defined by the user.
    input_data["zscore_time"] = input_data.groupby(["site","Source","direction","time"])["flow_total"].transform(zscore)
    input_data = input_data[input_data["zscore_time"]< 2]
    
    #store number of values left after first filter to calculate data loss
    z_time_values = len(input_data.index)
    print("Number of records removed with z_time: {}".format(not_null_values-z_time_values))

    #Remove sets that don't have at least 3 records.
    record_filter = lambda x: x["flow_total"].count() >= 3    
    input_data = input_data.groupby(["site","Source","direction","time","year"]).filter(record_filter)
    
    #store number of values left after final filter to calclate data loss
    record_filter_values = len(input_data.index)
    print("Number of records removed with record filter: {}".format(z_time_values-record_filter_values))

    #print % data loss
    print("Total number of removed records: {} ({:.2f}%)".format(not_null_values-record_filter_values,(not_null_values-record_filter_values)/not_null_values*100))

    #Generate matrix with final records and data coverage by comparing pre and post outlier filtering
    final_sample_size = input_data.groupby(["site","Source","direction","year","month"]).count()
    final_sample_size_pivot = final_sample_size.pivot_table(index=["site","Source","direction"],columns =["year","month"],values = "day")                    
    final_data_coverage = final_sample_size_pivot/not_null_sample_size_pivot

    input_DB_name = input_DB[:-4]
    #Export the dataframe to a .csv file
    input_data = input_data.drop(columns = ["month", "day", "zscore_time"])
    input_data.to_csv(f"{output_dir}\\{input_DB_name}_filtered_outliers_database.csv", index = False)
    final_sample_size_pivot.to_csv(f"{output_dir}\\{input_DB_name}_final_sample_size_pivot.csv")
    final_data_coverage.to_csv(f"{output_dir}\\{input_DB_name}_Outlier_filter_percentage.csv")


def filter_ATC_outliers(input_dir, output_dir):
    """ Applies the outlier filter to all databases in a given neutral day directory, outputting results to another given directory"""
    
    for year in ["2019", "2022", "2023"]:
        os.chdir(f"{input_dir}\\{year}")
        files = glob.glob("*.csv")

        for file in files:
            print(f"Filtering File: {file}")
            if year == "2023":
                outlier_filter_2023(file, f"{output_dir}\\{year}")
            else:
                outlier_filter(file,f"{output_dir}\\{year}")
    
def filter_ATC_outliers_b_combined_dbs(input_dir, output_dir):
    """ 
    Applies the outlier filter to all databases in a given neutral day directory, outputting results to another given directory
    
    Note the main difference to the previous function is the folder structure. Because there is only one file for each year
    they are put into the same input directory and the directory remains unchanged within the year loop
    """
    os.chdir(input_dir)
    for year in ["2019", "2022", "2023"]: #
        
        file = f"ATC_{year}_Combined.csv"

        print(f"Filtering File: {file}")
        if year == "2023":
            outlier_filter_2023(file, f"{output_dir}\\{year}")
        else:
            outlier_filter(file,f"{output_dir}\\{year}")





if __name__ == "__main__":
    """
    Calls each function in turn, creating filtered databases both for 15m and hourly seperately and combined into one hourly database
    
    HCC also requested Severnside filtered data on its own. Hence the last three lines
    """

    os.chdir(os.getcwd())


    ####1. APPEND FORMATTED DBs and put into folder 3###
    #append_ATC_dbs()
    
    ####2. FILTER ALL APPENDED DBs for neutral days and put into folder 4_1###
    #filter_ATC_DBs_Mon_Thurs(File_directories.ATC_APPENDED, File_directories.DATE_FILTER_MON_THURS_NEUTRAL, File_directories.ATC_MON_THURS_NEUTRAL)

    ####3. FILTER UNCOMBINED DBs for outliers and put into folder 4_2###
    #filter_ATC_outliers(File_directories.ATC_MON_THURS_NEUTRAL,File_directories.ATC_OUTLIER_FILTERED)

    ####4. COMBINE 15M AND HOURLY NEUTRAL DAY DBs and put into folder 4_1_1b###
    #combine_all_neutral_day_dbs(File_directories.ATC_MON_THURS_NEUTRAL, File_directories.ATC_NEUTRAL_COMBINED)

    ####5. FILTER COMBINED DBs for outliers and put into folder 4_2_2b###
    #filter_ATC_outliers_b_combined_dbs(File_directories.ATC_NEUTRAL_COMBINED, File_directories.ATC_COMBINED_OUTLIER_FILTERED)
    
    #Retrieve filtered severnside data:
    data = pd.read_csv(f"{File_directories.ATC_OUTLIER_FILTERED}\\2023\\ATC_2023_15m_appended_neutral_mon-thurs_filtered_outliers_database.csv")
    data = data.loc[data["Source"] == "Severnside ATC"]
    data.to_csv("Severnside_Filtered_Database.csv")