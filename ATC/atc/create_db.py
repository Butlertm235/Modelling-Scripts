# -*- coding: utf-8 -*-
"""
Created on Tue May 26 08:22:00 2020

@author: UKECF002

"""
import os
import pandas as pd
import numpy as np
import glob

def tabular_flows(df2, key, hour):
    """Reads the flows matrix and converts it to tabular format. It then creates columns for
    day of the week, day, month, year and combines them into a date column. Non-existing days for months
    with less than 31 days are removed.
    
    :param df2: dataframe with a single flow matrix where rows represent day of the month and columns
    represent hour of the day
    :result: dataframe with flows in tabular format"""
    
    df2 = pd.melt(df2.reset_index(),id_vars="index",value_vars=list(df2.columns),value_name="flow")
    #Split day of the week + day number into two columns
    df2[["day_week","day"]] = df2["index"].str.split(" ",expand=True)
    df2 = df2.drop(columns=["index"])
    #Remove non-existing days for those months with less than 31 days (i.e. the code always reads 31 rows,
    #but in February we would need to remove the last 2/3 rows)
    df2 = df2.dropna(subset=["day"])
    #Add columns for month and year, and create a datetime object
    df2 = pd.merge(df2,hour,how="inner",on="variable")
    df2["month"]=key[9:11]
    df2["year"]=key[-4:]
    df2["date"]=pd.to_datetime(df2[["year","month","day","hour"]],format = '%Y/%M/%D %H')
    df2 = df2.drop(columns="variable")
    df2 = df2.set_index("date")
    return df2


def create_db(db_dir,db_name):
    """Reads Suffolk ATC flow spreadsheets and combines them into a single dataframe
    that is ready for processing.
    
    This function is intended to be used only with Suffolk ATC files provided in
    their specific format. If raw data has not been recorded (blank cells in the 
    spreadsheets) this will be loaded as NaN in the dataframe.
    
    :param db_dir: path of Suffolk ATC files (include r before the path)
    :param db_name: name of the output csv file
    :result: dataframe with the combined data from all files. Dataframe will also be
    exported as a .csv file with the name "survey_database.csv" """
    
    ####################
    #Start of the function
    ####################
    
    path=os.getcwd()

    hour = pd.read_csv("hour.csv")
    
    os.chdir(db_dir)
    #read all excel files from the folder
    files = glob.glob("*.xlsx")
    
    #Create empty database dataframe
    survey_database = pd.DataFrame()
    
    # Survey direction
    direction = [1,2]
    #Iteration over the spreadsheets
    for i in files:
        #name of the ATC site
        name=i[13:21]
        #empty site dataframe
        site = pd.DataFrame()
        file_path = os.path.join(db_dir,i)
        
        #Iteration over direction
        for j in direction:
            #empty direction dataframe
            df_direction = pd.DataFrame()
            if j == 1:
                skipr = 5
            else:
                skipr = 46       
            #Read each tab of the spreadsheet and save it in a dictionary
            df = pd.read_excel(file_path, sheet_name=None,skiprows=skipr,index_col=0,nrows=31)
     
            #Iteration over the tabs
            for key in df:
                df2=df[key]
                if df2.empty:
                    break
                #Reformat the data to tabular format
                df2 = tabular_flows(df2, key, hour)
                #Append the tab data to the direction dataframe
                df_direction = df_direction.append(df2)
            #Add a column with the direction
            df_direction["direction"] = j
            #Append the direction data to the site dataframe
            site = site.append(df_direction)
        #Add a column with the site name
        site["site"] = name
        #Append the site data to the database 
        survey_database = survey_database.append(site)
    
    survey_database = survey_database.rename_axis('date').reset_index()
    os.chdir(path)


    
    #Export the dataframe to a .csv file
    export_csv = survey_database.to_csv(db_name+".csv",index=False)
    survey_db_lite = survey_database.drop(columns=["day_week","day","hour","month","year"])
    export_csv = survey_db_lite.to_csv(db_name+"lite.csv",index=False)
    
    ####################
    #End of the function
    ####################
    
    return survey_database



def create_db_webtris (db_dir,db_name):
    """Reads WebTRIS daily reports and combines them into a single dataframe
    that is ready for processing.
    
    This function is intended to be used only with WebTRIS daily reports in
    their specific format. If raw data has not been recorded (blank cells in the 
    spreadsheets) this will be loaded as NaN in the dataframe.
    
    :param db_dir: path of Suffolk ATC files (include r before the path)
    :result: dataframe with the combined data from all files. Dataframe will also be
    exported as a .csv file with the name "survey_database.csv" """
    
    ####################
    #Start of the function
    ####################
    
    path=os.getcwd()
    
    os.chdir(db_dir)
    #read all excel files from the folder
    files = glob.glob("*.csv")
    
    #Create empty database dataframe
    survey_database = pd.DataFrame()
    
    #Iteration over the spreadsheets
    for i in files:
        df = pd.read_csv(i,parse_dates = [["Report Date","Time Period Ending"]])
        df = df.drop(columns=["0 - 10 mph","11 - 15 mph","16 - 20 mph","21 - 25 mph","26 - 30 mph","31 - 35 mph","36 - 40 mph","41 - 45 mph","46 - 50 mph","51 - 55 mph","56 - 60 mph","61 - 70 mph","71 - 80 mph","80+ mph","Time Interval"])
        df = df.rename(columns={"Report Date_Time Period Ending":"date","0 - 520 cm":"flow_car","521 - 660 cm":"flow_LGV","661 - 1160 cm":"flow_OGV1","1160+ cm":"flow_OGV2","Total Volume":"flow_total"})
        df["flow_HGV"] = df["flow_OGV1"]+df["flow_OGV2"]
        np_sum = lambda x: x.values.sum()
        df = df.resample('H', on='date').agg({"flow_car":np_sum,"flow_LGV":np_sum,"flow_OGV1":np_sum,"flow_OGV2":np_sum,"flow_HGV":np_sum,"flow_total":np_sum,"Avg mph":np.average,"site":"first"})
        survey_database = survey_database.append(df)
        
    survey_database = survey_database.rename_axis('date').reset_index()
    os.chdir(path)
    export_csv = survey_database.to_csv(db_name+".csv",index=False)
    
    ####################
    #End of the function
    ####################
    
    return survey_database


def create_db_dft (db_dir,db_name):
    """Reads DfT raw counts and combines them into a single dataframe
    that is ready for processing.
    
    This function is intended to be used only with DfT data in
    their specific format. If raw data has not been recorded (blank cells in the 
    spreadsheets) this will be loaded as NaN in the dataframe.
    
    :param db_dir: path of Suffolk ATC files (include r before the path)
    :result: dataframe with the combined data from all files. Dataframe will also be
    exported as a .csv file with the name "survey_database.csv" """
    
    ####################
    #Start of the function
    ####################
    
    path=os.getcwd()
    
    os.chdir(db_dir)
    #read all excel files from the folder
    files = glob.glob("*.csv")
    
    #Create empty database dataframe
    survey_database = pd.DataFrame()
    
    #Iteration over the spreadsheets
    for i in files:
        df = pd.read_csv(i,parse_dates =["count_date"])
        df["hour_delta"] = pd.to_timedelta(df["hour"],unit="h")
        df["date"] = df["count_date"] + df["hour_delta"]
        df["flow_OGV1"] = df["hgvs_2_rigid_axle"] + df["hgvs_3_rigid_axle"]
        df["flow_OGV2"] = df["hgvs_4_or_more_rigid_axle"] + df["hgvs_3_or_4_articulated_axle"] + df["hgvs_5_articulated_axle"] + df["hgvs_6_articulated_axle"]
        df["flow_HGV"] = df["flow_OGV1"]+df["flow_OGV2"]
        df = df.rename(columns={"cars_and_taxis":"flow_car","lgvs":"flow_LGV","count_point_id":"site","direction_of_travel":"direction"})
        df["flow_total"] = df["flow_car"]+ df["flow_LGV"] + df["flow_HGV"]
        df = df.drop(columns=["year","region_id","local_authority_id","road_name","road_category","road_type","start_junction_road_name","end_junction_road_name","easting","northing","latitude","longitude","link_length_km","link_length_miles","sequence","ramp","pedal_cycles","buses_and_coaches","hgvs_2_rigid_axle","hgvs_3_rigid_axle","hgvs_4_or_more_rigid_axle","hgvs_3_or_4_articulated_axle","hgvs_5_articulated_axle","hgvs_6_articulated_axle","all_hgvs","all_motor_vehicles","count_date","hour","two_wheeled_motor_vehicles","hour_delta","id","count_point_id"])
        survey_database = survey_database.append(df)
    survey_database["source"] = "DfT"    
    os.chdir(path)
    export_csv = survey_database.to_csv(db_name+".csv",index=False)
    
    ####################
    #End of the function
    ####################
    
    return survey_database