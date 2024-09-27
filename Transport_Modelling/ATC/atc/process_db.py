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