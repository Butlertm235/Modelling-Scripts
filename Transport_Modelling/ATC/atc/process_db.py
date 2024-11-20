# -*- coding: utf-8 -*-
"""
Created on Wed Jun 10 08:35:39 2020

@author: UKECF002
"""
import pandas as pd

def date_filter(db,date):
    """
    Reads the database (db) as a dataframe and then applies a filter based on date

    Parameters
    ----------
    db : dataframe
        Dataframe with the raw survey data.
    date : string
        Name of the csv file with the date filter.

    Returns
    -------
    df: dataframe       
           Dataframe with the filtered dates.
    """
    
    df = db.copy()
    df["date_only"] = pd.to_datetime(df[["year","month","day"]], format='%Y-%M-%D')
    dates = pd.read_csv(date,parse_dates=["date"],dayfirst = True)
    df = pd.merge(df,dates,how="inner",left_on="date_only",right_on="date")
    df = df.drop(columns="date_only")

    return df

def date_filter_v2(db,date):
    """
    Reads the database (db) as a dataframe and then applies a filter based on date

    Parameters
    ----------
    db : dataframe
        Dataframe with the raw survey data.
    date : string
        Name of the csv file with the date filter.

    Returns
    -------
    df: dataframe       
           Dataframe with the filtered dates.
    """
    
    df = db.copy()
    #df["date_only"] = pd.to_datetime(df[["year","month","day"]], format='%Y-%M-%D')    
    df["date_only"] = pd.to_datetime(df.date).dt.date
    # df["hour"] = pd.to_datetime(df.date).dt.hour
    # df["month"] = pd.to_datetime(df.date).dt.month
    # df["day"] = pd.to_datetime(df.date).dt.day
    
    dates = pd.read_csv(date,parse_dates=["date"],dayfirst = True)
    dates["date_only"] = pd.to_datetime(dates.date).dt.date
    
    dates = dates.drop(columns="date")
    df = pd.merge(df,dates,how="inner",left_on="date_only",right_on="date_only")
    df = df.drop(columns="date_only")

    return df


def tp_filter(db,tp):
    """
    Reads the database (db) as a dataframe and then applies a filter based on time period

    Parameters
    ----------
    db : dataframe
        Dataframe with the raw survey data.
    tp : string
        Name of the csv file with the time period filter.

    Returns
    -------
    df: dataframe       
           Dataframe with the filtered time periods.
    """
    
    df = db.copy()
    tps = pd.read_csv(tp)
    df = pd.merge(df,tps,how="inner",on="hour")

    return df


def site_filter(db,site):
    """
    Reads the database (db) as a dataframe and then applies a filter based on ATC site

    Parameters
    ----------
    db : dataframe
        Dataframe with the raw survey data.
    date : string
        Name of the csv file with the date filter.

    Returns
    -------
    df: dataframe       
           Dataframe with the filtered sites.
    """
    
    df = db.copy()
    sites = pd.read_csv(site)
    df = pd.merge(df,sites,how="inner",on="site")

    return df
 
   
def outlier_filter(db,z_hour,z_month):
    """Reads the database (db) as a dataframe and then applies the following filters:
       Groups the dataframe by ATC site, direction and hour and applies the z score filter defined in z_hour
       Groups the dataframe by ATC site, direction, hour, year and month and applies the z score filter defined in z_month
       Groups the dataframe by ATC site, direction, hour, year and month and removes those sets that have less than 3 records
       
       z_hour should be equal or higher than z_month. When there is a markedly seasonal variation z_hour should be significantly higher
       than z_month, as otherwise it will remove values that deviate from the mean due to seasonal variation rather than being outliers.

        Parameters
        ----------       
       db: dataframe 
           Dataframe with the raw survey data.
       z_hour: float
           z score to filter outliers at site/direction/hour level
       z_month: float
           z score to filter outliers at site/direction/hour/year/month level

       Returns
       -------
       df: dataframe       
           Dataframe with the outliers filtered out.
       """
        
    df = db.copy()
    
    #Generate matrix with initial records before removing outliers
    not_null_sample_size = df.groupby(["site","direction","year","month"]).count()
    not_null_sample_size_pivot = not_null_sample_size.pivot_table(index=["site","direction"],columns =["year","month"],values = "day")
    not_null_values = len(df.index)
    
    #Definition of the z score
    zscore = lambda x: abs(x-x.mean())/x.std()
    
    #Calculation of the z score for the hour group. Filtering of values above the threshold defined by the user.
    df["zscore_hour"] = df.groupby(["site","direction","hour"])["flow"].transform(zscore)
    df = df[df["zscore_hour"]< z_hour]
    z_hour_values = len(df.index)
    print("Number of records removed with z_hour: {}".format(not_null_values-z_hour_values))
    
    #Calculation of the z score for the month group. Filtering of values above the threshold defined by the user.
    df["zscore_month"] = df.groupby(["site","direction","hour","year","month"])["flow"].transform(zscore)
    df = df[df["zscore_month"]< z_month]
    z_month_values = len(df.index)
    print("Number of records removed with z_month: {}".format(z_hour_values-z_month_values))
    
    #Remove sets that don't have at least 3 records.
    record_filter = lambda x: x["flow"].count() >= 3    
    df = df.groupby(["site","direction","hour","year","month"]).filter(record_filter)
    record_filter_values = len(df.index)
    print("Number of records removed with record filter: {}".format(z_month_values-record_filter_values))
    
    print("Total number of removed records: {} ({:.2f}%)".format(not_null_values-record_filter_values,(not_null_values-record_filter_values)/not_null_values*100))
    
    #Generate matrix with final records and data coverage by comparing pre and post outlier filtering
    final_sample_size = df.groupby(["site","direction","year","month"]).count()
    final_sample_size_pivot = final_sample_size.pivot_table(index=["site","direction"],columns =["year","month"],values = "day")                    
    final_data_coverage = final_sample_size_pivot/not_null_sample_size_pivot
    
    
    #Export the dataframe to a .csv file
    export_csv = df.to_csv("filtered_outliers_database.csv")
    export_csv = final_sample_size_pivot.to_csv("final_sample_size_pivot.csv")
    export_csv = final_data_coverage.to_csv("Outlier_filter_percentage.csv")

    return df

def data_coverage(database,output_directory="",output_name_suffix=""):
    """
    This function provides the data coverage for each month at each site.

    Parameters
        ----------       
       database : dataframe 
           Csv file containing the database to find the data converage of
        output_directory : string
            Directory to output the coverage statistics
        output_name_suffix : string
            Suffix to relate the output csvs to the input database
    
    """
    df = pd.read_csv(database,index_col=0,parse_dates=["date"],dayfirst = True)

    #Generate matrix with maximum number of possible sample records
    full_sample_size = df.groupby(["site","month"]).count()
    full_sample_size_pivot = full_sample_size.pivot_table(index=["site"],columns =["month"],values = "day")
    col_list=list(full_sample_size_pivot)
    full_sample_size_pivot["all_months"] = full_sample_size_pivot.sum(axis=1,numeric_only=True)

    #Drop rows with no data
    df = df.dropna(subset=["flow_total"])

    #Generate matrix with sample records after removing blank data
    not_null_sample_size = df.groupby(["site","month"]).count()
    not_null_sample_size_pivot = not_null_sample_size.pivot_table(index=["site"],columns =["month"],values = "day")
    not_null_sample_size_pivot["all_months"] = not_null_sample_size_pivot.sum(axis=1,numeric_only=True)

    #Generate matrix with data coverage percentage
    data_coverage = not_null_sample_size_pivot/full_sample_size_pivot
    data_coverage = data_coverage.fillna(0)

    #Export the dataframe to a .csv file
    export_csv = full_sample_size_pivot.to_csv(output_directory+"full_sample_size_pivot_"+output_name_suffix+".csv")
    export_csv = not_null_sample_size_pivot.to_csv(output_directory+"not_null_sample_size_pivot_"+output_name_suffix+".csv")
    export_csv = data_coverage.to_csv(output_directory+"data_coverage_"+output_name_suffix+".csv")

def data_coverage_v2(database,output_directory="",output_name_suffix=""):
    """
    This function provides the data coverage for each month at each site.

    Parameters
        ----------       
       database : dataframe 
           Csv file containing the database to find the data converage of
        output_directory : string
            Directory to output the coverage statistics
        output_name_suffix : string
            Suffix to relate the output csvs to the input database
    
    """
    df = pd.read_csv(database,parse_dates=["date"],dayfirst = True)

    #Generate matrix with maximum number of possible sample records
    full_sample_size = df.groupby(["source","site","month"]).count()    
    full_sample_size_pivot = full_sample_size.pivot_table(index=["source","site"],columns =["month"],values = "date")
    col_list=list(full_sample_size_pivot)
    full_sample_size_pivot["all_months"] = full_sample_size_pivot.sum(axis=1,numeric_only=True)

    #Drop rows with no data
    df = df.dropna(subset=["flow_total"])

    #Generate matrix with sample records after removing blank data
    not_null_sample_size = df.groupby(["source","site","month"]).count()
    not_null_sample_size_pivot = not_null_sample_size.pivot_table(index=["source","site"],columns =["month"],values = "date")
    not_null_sample_size_pivot["all_months"] = not_null_sample_size_pivot.sum(axis=1,numeric_only=True)

    #Generate matrix with data coverage percentage
    data_coverage = not_null_sample_size_pivot/full_sample_size_pivot
    data_coverage = data_coverage.fillna(0)

    #Export the dataframe to a .csv file
    export_csv = full_sample_size_pivot.to_csv(output_directory+"full_sample_size_pivot_"+output_name_suffix+".csv")
    export_csv = not_null_sample_size_pivot.to_csv(output_directory+"not_null_sample_size_pivot_"+output_name_suffix+".csv")
    export_csv = data_coverage.to_csv(output_directory+"data_coverage_"+output_name_suffix+".csv")

def data_coverage_v3(database,output_directory="",output_name_suffix=""):
    """
    This function provides the data coverage for each month at each site.

    Parameters
        ----------       
       database : dataframe 
           Csv file containing the database to find the data converage of
        output_directory : string
            Directory to output the coverage statistics
        output_name_suffix : string
            Suffix to relate the output csvs to the input database
    
    """
    df = pd.read_csv(database,parse_dates=["date"],dayfirst = True)

    df["month"] = pd.to_datetime(df.date).dt.month

    #Generate data frame with number of directions for each site
    full_sample_size_dirn = df.groupby(["source","site","direction","month"]).count()    
    full_sample_size_dirn_pivot = full_sample_size_dirn.pivot_table(index=["source","site","direction"],columns =["month"],values = "date")    
    full_sample_size_dirn_df = full_sample_size_dirn_pivot.reset_index()
    direction_count=full_sample_size_dirn_df.groupby(["source","site"]).count().reset_index()["direction"]

    #Generate matrix with maximum number of possible sample records
    full_sample_size = df.groupby(["source","site","month"]).count()    
    full_sample_size_pivot = full_sample_size.pivot_table(index=["source","site"],columns =["month"],values = "date")    
    full_sample_size_df = full_sample_size_pivot.reset_index()
    full_sample_size_df["dirn_count"]=direction_count
    
    months = [3,4,5,6,7,9,10,11] #neutral months of year
    days_in_months = [18,7,7,16,11,16,14,18] #neutral days in each of these months (2023)
    
    for i in range(len(months)):
        full_sample_size_df.loc[full_sample_size_df["source"] == "Tagmaster", months[i]] = days_in_months[i]*full_sample_size_df["dirn_count"]*19 #19 'hours' in tagmaster
        full_sample_size_df.loc[full_sample_size_df["source"] != "Tagmaster", months[i]] = days_in_months[i]*full_sample_size_df["dirn_count"]*24 #24 hours in a day
   

    full_sample_size_df["all_months"] = full_sample_size_df[months].sum(axis=1)
    # full_sample_size_df["all_months"] = full_sample_size_df[3]+full_sample_size_df[4]+full_sample_size_df[5]+full_sample_size_df[6]+full_sample_size_df[7]+full_sample_size_df[9]+full_sample_size_df[10]+full_sample_size_df[11]

    df_not_null = df.dropna(subset=["flow_total"])

    #Generate matrix with maximum number of possible sample records
    not_null_sample_size = df_not_null.groupby(["source","site","month"]).count()    
    not_null_sample_size = not_null_sample_size.pivot_table(index=["source","site"],columns =["month"],values = "date")    
    not_null_sample_size_df = not_null_sample_size.reset_index()
    
    not_null_sample_size_df["all_months"] = not_null_sample_size_df[months].sum(axis=1)

       
    #not_null_sample_size_df["all_months"] = not_null_sample_size_df[3]+not_null_sample_size_df[4]+not_null_sample_size_df[5]+not_null_sample_size_df[6]+not_null_sample_size_df[7]+not_null_sample_size_df[9]+not_null_sample_size_df[10]+not_null_sample_size_df[11]


    # col_list=list(full_sample_size_pivot)
    #print(full_sample_size_df.groupby(["source","site"]).count().reset_index()["direction"])
    # #Drop rows with no data
    # df = df.dropna(subset=["flow_total"])

    # #Generate matrix with sample records after removing blank data
    # not_null_sample_size = df.groupby(["source","site","month"]).count()
    # not_null_sample_size_pivot = not_null_sample_size.pivot_table(index=["source","site"],columns =["month"],values = "date")
    # not_null_sample_size_pivot["all_months"] = not_null_sample_size_pivot.sum(axis=1,numeric_only=True)

    # #Generate matrix with data coverage percentage
    data_coverage = pd.merge(full_sample_size_df,not_null_sample_size_df,how='left',on=['source','site'])

    data_coverage['Mar'] = data_coverage['3_y']/data_coverage['3_x']
    data_coverage['Apr'] = data_coverage['4_y']/data_coverage['4_x']
    data_coverage['May'] = data_coverage['5_y']/data_coverage['5_x']
    data_coverage['Jun'] = data_coverage['6_y']/data_coverage['6_x']
    data_coverage['Jul'] = data_coverage['7_y']/data_coverage['7_x']
    data_coverage['Sep'] = data_coverage['9_y']/data_coverage['9_x']
    data_coverage['Oct'] = data_coverage['10_y']/data_coverage['10_x']
    data_coverage['Nov'] = data_coverage['11_y']/data_coverage['11_x']
    data_coverage['All_months'] = data_coverage['all_months_y']/data_coverage['all_months_x']

    data_coverage = data_coverage[['source','site','Mar','Apr','May','Jun','Jul','Sep','Oct','Nov','All_months']]
    
    


    # data_coverage = not_null_sample_size_df/full_sample_size_df
    data_coverage = data_coverage.fillna(0)

    #Export the dataframe to a .csv file
    export_csv = full_sample_size_df.to_csv(output_directory+"full_sample_size_pivot_"+output_name_suffix+".csv",index=None)
    
    export_csv = not_null_sample_size.to_csv(output_directory+"not_null_sample_size_pivot_"+output_name_suffix+".csv",index=None)
    export_csv = data_coverage.to_csv(output_directory+"data_coverage_"+output_name_suffix+".csv",index=None)