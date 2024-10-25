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
    
    df2 = pd.melt(df2.reset_index(),id_vars="index",value_vars=list(df2.columns),value_name="flow_total")
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


def create_db_ca_month_volume(db_dir,db_name):
    """Reads CA Traffic / Tagmaster month volume spreadsheets spreadsheets and combines them into a single dataframe
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
    #df to convert from string time to an integer number  
    hour = pd.DataFrame({"variable":["Bin 1\n0-5:00","Bin 2\n6:00","Bin 3\n7:00","Bin 4\n8:00","Bin 5\n9:00","Bin 6\n10:00","Bin 7\n11:00","Bin 8\n12:00","Bin 9\n13:00","Bin 10\n14:00","Bin 11\n15:00","Bin 12\n16:00","Bin 13\n17:00","Bin 14\n18:00","Bin 15\n19:00","Bin 16\n20:00","Bin 17\n21:00","Bin 18\n22:00","Bin 19\n23:00"],"hour":[0,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23]})
    
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
                df_direction = pd.concat([df_direction, df2])
            #Add a column with the direction
            df_direction["direction"] = j
            #Append the direction data to the site dataframe
            site = pd.concat([site,df_direction])
        #Add a column with the site name
        site["site"] = name
        #Append the site data to the database 
        survey_database = pd.concat([survey_database, site])
    
    survey_database = survey_database.rename_axis('date').reset_index()
    survey_database["source"] = "Tagmaster"
    os.chdir(path)


    
    #Export the dataframe to a .csv file
    export_csv = survey_database.to_csv(db_name+".csv",index=False)
    survey_db_lite = survey_database.drop(columns=["day_week","day","hour","month","year"])
    export_csv = survey_db_lite.to_csv(db_name+"lite.csv",index=False)
    
    ####################
    #End of the function
    ####################
    
    return survey_database

def create_db_ca_month_volume_v2(db_dir,db_name):
    """
    Reads CA Traffic / Tagmaster month volume spreadsheets spreadsheets and combines them into a single dataframe
    that is ready for processing.
    
    This function is intended to be used only with Suffolk ATC files provided in
    their specific format. If raw data has not been recorded (blank cells in the 
    spreadsheets) this will be loaded as NaN in the dataframe.

    This _v2 function iterates over the directions directly given in the table and includes these in the output.
    Where incomplete direction data is provided, this site is skipped.
    These skipped sites can be viewed in the '..._direction_data.csv' output file where 'no_tables' != 'no_directions'
    
    :param db_dir: path of Suffolk ATC files (include r before the path)
    :param db_name: name of the output csv file
    :result: dataframe with the combined data from all files. Dataframe will also be
    exported as a .csv file with the name "survey_database.csv" 
    """
    
    ####################
    #Start of the function
    ####################
    
    path=os.getcwd()
    #df to convert from string time to an integer number  
    hour = pd.DataFrame({"variable":["Bin 1\n0-5:00","Bin 2\n6:00","Bin 3\n7:00","Bin 4\n8:00","Bin 5\n9:00","Bin 6\n10:00","Bin 7\n11:00","Bin 8\n12:00","Bin 9\n13:00","Bin 10\n14:00","Bin 11\n15:00","Bin 12\n16:00","Bin 13\n17:00","Bin 14\n18:00","Bin 15\n19:00","Bin 16\n20:00","Bin 17\n21:00","Bin 18\n22:00","Bin 19\n23:00"],"hour":[0,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23]})
    
    os.chdir(db_dir)
    #read all excel files from the folder
    files = glob.glob("*.xlsx")
    
    #Create empty database dataframe
    survey_database = pd.DataFrame()
    
    # to get info on sites where direction not given
    direction_data=pd.DataFrame(columns=["file","no_tables","no_directions"])    

    #Iteration over the spreadsheets
    for i in files:
        print("site "+i+" starting...")
        #name of the ATC site
        name=i[13:21]
        #empty site dataframe
        site = pd.DataFrame()
        file_path = os.path.join(db_dir,i)

        # Get an array of channels for this spreadsheet
        first_sheet = pd.read_excel(file_path,header=None)
        channels = first_sheet[19][first_sheet[18]=="Channel:"]

        # to get info on sites where direction not given
        tables = first_sheet[0][first_sheet[0]=="CA Traffic"]        
        direction_data = direction_data._append({"file":i,"no_tables":len(tables),"no_directions":len(channels)},ignore_index=True)
        
        # extract the latitude and longitude coordinates
        # lats = first_sheet[19][first_sheet[18]=="Lat/Lng."]
        # longs = first_sheet[20][first_sheet[18]=="Lat/Lng."]
        # site_names = first_sheet[0][first_sheet[18]=="Channel:"]

        # Survey direction (initial, arbitrary)
        direction = 1
        skipr = 5

        if len(tables)==len(channels):
        
            #Iteration over channels
            for channel in channels:
                #empty direction dataframe
                df_channel = pd.DataFrame()
    
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
                    df_channel = pd.concat([df_channel, df2])
                #Add a column with the direction
                df_channel["direction"] = direction
                

                #Add columns with the channel
                df_channel["channel"] = channel         

                # df_channel["latitude"] = lats[41*(direction-1)]
                # df_channel["longitude"] = longs[41*(direction-1)]            
                # df_channel["site_name"] = site_names[41*(direction-1)+1]

                #Append the direction data to the site dataframe
                site = pd.concat([site,df_channel])

                #indices for next iteration
                direction+=1
                skipr+=41

            #Add a column with the site name
            site["site"] = name
            #Append the site data to the database 
            survey_database = pd.concat([survey_database, site])
        print("site "+i+" processed and appended")
    
    survey_database = survey_database.rename_axis('date').reset_index()
    survey_database["source"] = "Tagmaster"
    os.chdir(path)


    
    #Export the dataframe to a .csv file
    export_csv = survey_database.to_csv(db_name+".csv",index=False)
    survey_db_lite = survey_database.drop(columns=["day_week","day","hour","month","year"])
    export_csv = survey_db_lite.to_csv(db_name+"lite.csv",index=False)

    # to export info on sites where direction not given
    direction_data["diff"]=direction_data["no_tables"]-direction_data["no_directions"]
    direction_data.to_csv(db_name+"_direction_data.csv",index=False)
    
    ####################
    #End of the function
    ####################
    
    return survey_database

def create_db_lat_long_tagmaster(db_dir,output_dir):

    """
    TEXT
    
    :param db_dir: path of Suffolk ATC files (include r before the path)
    :param db_name: filepath of directory the output csv file
    :result: dataframe with the combined data from all files. Dataframe will also be
    exported as a .csv file with the name "survey_database.csv" 
    """
    
    data_path= db_dir
    os.chdir(data_path)

    #read all excel files from the folder
    files = glob.glob("*.xlsx")

    #Create empty database dataframe
    survey_database = pd.DataFrame()

    #Iteration over the spreadsheets
    for i in files:
        name=i[13:21]
        #empty site dataframe
        site = pd.DataFrame()
        #Add a column with the site name
        site["site"] = name
        file_path = os.path.join(data_path,i)
        

        #empty direction dataframe
        df_direction = pd.DataFrame()

        df = pd.read_excel(file_path, sheet_name=1,usecols="A,T:U",index_col=None,nrows=2,names=[1,2,3],header=None)
        try:
            site_dir = df.values  
            #print(df.values)            
            df_direction = pd.DataFrame({"latitude":[site_dir[0,1]],"longitude":[site_dir[0,2]],"site_name":[site_dir[1,0]]})

            #Append the direction data to the site dataframe
            site = site._append(df_direction)
        except:
            pass
        #Add a column with the site name
        site["site"] = name
        
        #Append the site data to the database 
        survey_database = survey_database._append(site)

    #Export the dataframe to a .csv file
    export_csv = survey_database.to_csv(output_dir+"tagmaster_lat_long.csv",index=None)
    return survey_database

def create_db_ca_15_min_classified(db_dir,db_name):
    """Reads CA Traffic / Tagmaster classified 15 min spreadsheets and combines them into a single dataframe
    that is ready for processing.
    
    This function is intended to be used only with CA/Tagmaster files provided in
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
    #df to convert from string time to an integer number  
    #hour = pd.DataFrame({"variable":["Bin 1\n0-5:00","Bin 2\n6:00","Bin 3\n7:00","Bin 4\n8:00","Bin 5\n9:00","Bin 6\n10:00","Bin 7\n11:00","Bin 8\n12:00","Bin 9\n13:00","Bin 10\n14:00","Bin 11\n15:00","Bin 12\n16:00","Bin 13\n17:00","Bin 14\n18:00","Bin 15\n19:00","Bin 16\n20:00","Bin 17\n21:00","Bin 18\n22:00","Bin 19\n23:00"],"hour":[0,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23]})
    
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
        name=i[15:23]
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
                skipr = 123       
            #Read each tab of the spreadsheet and save it in a dictionary
            df = pd.read_excel(file_path, sheet_name=None,skiprows=skipr,index_col=0,nrows=96)
     
            #Iteration over the tabs
            for key in df:
                df2=df[key]
                if df2.empty:
                    break
                #Reformat the data to tabular format
                #df2 = tabular_flows(df2, key, hour)
                df2 = df2.reset_index()
                df2[["hour","minute","second"]] = df2["index"].astype(str).str.split(":",expand=True)
                df2 = df2.reset_index()
                df2["day"] = key[6:8]
                df2["month"]=key[9:11]
                df2["year"]=key[-4:]
                df2["date"]=pd.to_datetime(df2[["year","month","day","hour","minute"]],format = '%yyyy/%mm/%dd %HH%MM')
                #Append the tab data to the direction dataframe
                df_direction = pd.concat([df_direction, df2])
            #Add a column with the direction
            df_direction["direction"] = j
            #Append the direction data to the site dataframe
            site = pd.concat([site,df_direction])
        #Add a column with the site name
        site["site"] = name
        #Append the site data to the database 
        survey_database = pd.concat([survey_database, site])
    
    #survey_database = survey_database.rename_axis('date').reset_index()
    survey_database = survey_database.drop(columns=["year","month","day","hour","minute","second"])
    survey_database["source"] = "Tagmaster"
    os.chdir(path)


    
    #Export the dataframe to a .csv file
    export_csv = survey_database.to_csv(db_name+".csv",index=False)
    #survey_db_lite = survey_database.drop(columns=["day_week","day","hour","month","year"])
    #export_csv = survey_db_lite.to_csv(db_name+"lite.csv",index=False)
    
    ####################
    #End of the function
    ####################
    
    return survey_database

def create_db_webtris (db_dir,db_name,output_directory=""):
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
        np_sum = lambda x: x.values.sum()
        df = df.resample('H', on='date').agg({"flow_car":np_sum,"flow_LGV":np_sum,"flow_OGV1":np_sum,"flow_OGV2":np_sum,"flow_total":np_sum,"Avg mph":np.average,"site":"first"})
        survey_database = survey_database._append(df)
        
    survey_database = survey_database.rename_axis('date').reset_index()
    survey_database["source"] = "WebTRIS"
    os.chdir(path)
    export_csv = survey_database.to_csv(output_directory+db_name+".csv",index=False)
    
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


def create_db_vivacity(db_dir,db_name,output_directory=""):
    """Reads Vivacity hourly volume csv and combines them into a single dataframe
    that is ready for processing.
    
    This function is intended to be used only with Vivacity ATC files provided in
    hourly format.
    
    :param db_dir: path of Suffolk ATC files (include r before the path)
    :param db_name: name of the output csv file
    :result: dataframe with the combined data from all files. Dataframe will also be
    exported as a .csv file with the name "db_name.csv" """
    
    ####################
    #Start of the function
    ####################
    
    path=os.getcwd()
        
    os.chdir(db_dir)
    #read all excel files from the folder
    files = glob.glob("*.csv")
    
    #Create empty database dataframe
    survey_database = pd.DataFrame()
    
    #Iteration over the csvs
    for i in files:
        #name of the ATC site
        df = pd.read_csv(i,parse_dates=["UTC Datetime","Local Datetime"])
        survey_database = survey_database._append(df)
    survey_database = survey_database.drop(columns=["UTC Datetime","countlineName"])
    survey_database = survey_database.rename(columns={"Car":"flow_car","LGV":"flow_LGV","OGV1":"flow_OGV1","OGV2":"flow_OGV2","Bus":"flow_bus","Motorbike":"flow_motorbike","Cyclist":"flow_cyclist","Local Datetime":"date","countlineId":"site","Pedestrian":"flow_pedestrian"})
    survey_database["flow_total"] = survey_database["flow_car"] + survey_database["flow_cyclist"] + survey_database["flow_motorbike"] + survey_database["flow_bus"] + survey_database["flow_OGV1"] + survey_database["flow_OGV2"] + survey_database["flow_LGV"]
    survey_database["source"] = "Vivacity"
    os.chdir(path)

    #Export the dataframe to a .csv file
    export_csv = survey_database.to_csv(output_directory+db_name+".csv",index=False)
    
    ####################
    #End of the function
    ####################
    
    return survey_database