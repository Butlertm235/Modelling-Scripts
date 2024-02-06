import pandas as pd
import os
import glob
import numpy as np
import datetime
import re
import File_directories
import shutil

""" 
    ********
    FIRST THING TO DO:

    If this is the first time you have run the program since cloning the repository, run the first two functions first to copy the required raw data and other empty
    folders from their location in central data. This will ensure the given directories in File_directories.py work with all the other given functions. Do this by
    uncommentting the first two function calls in the main function of this file.

    UNCOMMENT THESE AGAIN AFTER RUNNING to avoid overwriting all the files stored in the resulting folders, from running the other functions.
    ********




   The rest of the file contains scripts used to convert raw ATC data to a standardised format. The final function should be
   adjusted to run the required formatting functions.
   
   The naming convention of the functions follows that of the raw data directories named in the File_directories.py file:

   e.g.
   format_ATC_2022_C_2_1() is used to format non-classified (C) 2022 raw data stored in the folder reached by following folder numbers 2 and then 1 within that directory

   format_ATC_HCC_C_1_2() has no specified year. When this is the case, assume that it can be used to format 
   raw data for more any year. therefore it can be used to format HCC's Classified (C) data,
   within directory found by following folder numbers 1 and then 2.

   A possible improvement to these functions would be to add an option for a resample into 1 hour intervals to the variables for the 15m functions
   A separate function could then be used to format depending on whether the required database is 15m or hourly intervals
   """

def copy_raw_data():
    """ 
    This is used to copy all required raw data from central data onto the C: Drive location of the python project.
    
    Only re-run when more data is added to central data or a new Github repository clone is made.
    """
    shutil.copytree(File_directories.RAW_DATA_CENTRAL_DATA_DIRECTORY, File_directories.RAW_DATA_C_DIRECTORY, dirs_exist_ok = True)

def copy_template_directory():
     """
     Used to copy the rest of the required folders and utility files over from C: Drive.

     This should ONLY be run after the clone of the github repository is made, to avoid overwriting all data processing with empty folders.
     """

     shutil.copytree(File_directories.TEMPLATE_FOLDER_STRUCTURE_CENTRAL_DATA_DIRECTORY, File_directories.PROGRAM_DIRECTORY, dirs_exist_ok = True)


def format_ATC_HCC_NC_1(input_dir: str, output_dir: str, discarded_dir: str, year: str):
    
    #define required variables
    os.chdir(input_dir)
    discarded_data = pd.DataFrame()
    files = glob.glob("*.xlsx")
    #final database:
    survey_database = pd.DataFrame()
    #time periods for non-count checks
    AM_PEAK_PERIOD = ["07:00", "8:00", "9:00"]
    PM_PEAK_PERIOD = ["16:00", "17:00", "18:00"]

    #loop through files, defining file path for each one
    for file in files:
        print("Adding ",file," to the database")
        file_path = os.path.join(input_dir,file)
        
        #retrieve site number
        sitedf = pd.read_excel(file_path,skiprows=0,usecols="A",index_col=None,nrows=0)
        site = str(sitedf.columns.values[0])
        site = site[-3:]
        
        #read up to a maximum of 100 tables in turn skipping 45 rows for each one and reading the direction for each one.
        for table in range (100):
                skipr = 6+table*45
                dirdf = pd.read_excel(file_path,skiprows = skipr - 4,usecols="G",index_col=None,nrows=0)

                #if the cell including the direction is left blank, break the loop and move onto the next file
                if len(dirdf. columns) == 0:
                    break
                
                #read direction from direction dataframe
                dir = dirdf.columns.values[0]

                #skip unneeded tables
                if dir == "Channel: Total Flow":
                    continue
                if dir == "Channel: Errors":
                    continue
                if "Cycles" in dir:
                    continue
                if "Traffic" in dir:
                    continue
                
                #read table of data
                df = pd.read_excel(file_path,skiprows = skipr -1,nrows=24,dtype={0:str})
                #define dataframe for data collection for entire table in the desired format
                formatdf = pd.DataFrame(columns = ['flow_total'])

                #For each day of the week...
                for day in range(7):

                    #read the date off of the corresponding column
                    survey_date = str(df.columns.values[day+1].date())

                    #define new dataframe for each day
                    day_df = pd.DataFrame(columns = ['date','flow_total'])
                    #append date and time and assign to date collumn and convert to datetime object
                    day_df['date'] = survey_date + " " + df['Begin']
                    day_df['date']= pd.to_datetime(day_df['date'])

                    #copy flows from the table for corresponding day
                    day_df['flow_total'] = df[df.columns.values[day+1]]

                    #add site and direction to the day dataframe
                    day_df['site'] = site
                    day_df['direction'] = dir

                    #set datetime index so that .loc can be used to check for non counts
                    day_df = day_df.set_index(["date"])

                    #define list of AM and PM peak datetime objects to check on the dataframe
                    am_peak_datetime_objects = []
                    for peak_hour in AM_PEAK_PERIOD:
                        peak_hour = survey_date + " " + peak_hour
                        peak_hour = datetime.datetime.strptime(peak_hour, '%Y-%m-%d %H:%M')
                        am_peak_datetime_objects.append(peak_hour)

                    pm_peak_datetime_objects = []
                    for peak_hour in PM_PEAK_PERIOD:
                        peak_hour = survey_date + " " + peak_hour
                        peak_hour = datetime.datetime.strptime(peak_hour, '%Y-%m-%d %H:%M')
                        pm_peak_datetime_objects.append(peak_hour)

                    #set sum for AM peak period to zero before summing each peak period flow value
                    period_sum = 0

                    for peak_hour in am_peak_datetime_objects:
                        period_sum += day_df.loc[peak_hour, "flow_total"]
                        print(f"Time period {peak_hour} sum is: {period_sum}")
        
                    #if zero flow in AM peak, skip table and add to discarded data
                    if period_sum == 0:
                        discard_record = day_df[["site", "direction"]].head(1)
                        discarded_data = pd.concat([discarded_data, discard_record], ignore_index = True)
                        continue
                    
                    #reset period sum to zero before checking PM peak period flow total
                    period_sum = 0

                    for peak_hour in pm_peak_datetime_objects:
                        period_sum += day_df.loc[peak_hour, "flow_total"]
                        print(f"Time period {peak_hour} sum is: {period_sum}")
                    
                    #discard if total peak period flow is zero
                    if period_sum == 0:
                        discard_record = day_df[["site", "direction"]].head(1)
                        discarded_data = pd.concat([discarded_data, discard_record])
                        continue
                        
                    #add the data for individual date onto that holding data for the whole table
                    formatdf = pd.concat([formatdf,day_df])

                    #add remaining unused columns to dataframe
                    formatdf['Source'] = 'ATC'
                    formatdf['flow_car'] = ''
                    formatdf['flow_LGV'] = ''
                    formatdf['flow_OGV_1'] = ''
                    formatdf['flow_OGV_2'] = ''
                    formatdf['flow_HGV'] = ''
                
                #define NaN value to be replaced, corresponding to non-counts
                nan_value = float("NaN")
                #replace with blanks in the dataframe, before deleting
                formatdf.replace("", nan_value, inplace=True)
                formatdf.dropna(subset = ["flow_total"], inplace=True)


                #standardise direction naming
                if "Northeast" in dir:
                    formatdf["direction"] = "North East"

                elif "Northwest" in dir:
                    formatdf["direction"] = "North West"
                
                elif "Southeast" in dir:
                    formatdf["direction"] = "South East"
                
                elif "Southwest" in dir:
                    formatdf["direction"] = "South West"
                
                elif "East" in dir:
                    formatdf["direction"] = "East"

                elif "West" in dir:
                    formatdf["direction"] = "West"
                
                elif "North" in dir:
                    formatdf["direction"] = "North"
                
                elif "South" in dir:
                    formatdf["direction"] = "South"

                #reset index and rename date column
                formatdf = formatdf.reset_index()
                formatdf = formatdf.rename(columns={"index":"date"})

                #Add onto final database
                survey_database = pd.concat([survey_database,formatdf], ignore_index = True)
    #rename source field and output
    survey_database["Source"] = "HCC ATC"
    survey_database.to_csv(f"{output_dir}\\ATC_{year}_HCC_NC_1.csv", index = False)
    discarded_data.to_csv(f"{discarded_dir}\\Discarded_Data_ATC_{year}_HCC_NC_1.csv", index = False)


def format_ATC_NC_2(input_dir: str, output_dir: str, year: str):

    #define required variables
    os.chdir(input_dir)
    files = glob.glob("*.xlsx")

    #final database:
    survey_database = pd.DataFrame()

    #loop through files, defining list of sheet names (dates) for each one
    for file in files:

        print("Adding ",file," to the database")
        sheets = pd.ExcelFile(file)
        sheet_names = sheets.sheet_names

        #number of dates:
        a = len(sheet_names)
        
        #loop through dates
        for date in sheet_names[0:a]:
            
            #Record site name:
            sitedf = pd.read_excel(file,sheet_name=date,skiprows=0,usecols="B",index_col=None,nrows=0)
            site = str(sitedf.columns.values[0])
            site = site[-3:] 
        
            #Loop through directions, maximum of three tables in this format, skip 46 rows for each table
            for table in range (3):
                skipr = 5+table*46


                #Record direction by defining single cell dataframe
                dirdf = pd.read_excel(file,sheet_name=date,skiprows = skipr - 3,usecols="I",index_col=None,nrows=0)
                
                #if the cell including the direction is left blank, break the loop and move onto the next file
                if len(dirdf. columns) == 0:
                    break
                
                #read direction from single cell dataframe
                dir = dirdf.columns.values[0]

                #skip unneeded tables
                if dir == "":
                    continue
                if dir == "Total Flow":
                    continue
                if dir == "Errors":
                    continue
                #specific file contains unique direction naming convention
                if (site == "536" and year == "2022"):
                    if "bound" in dir:
                        continue
                #Read raw data tab;e
                df = pd.read_excel(file,sheet_name=date,skiprows = skipr ,nrows=24,dtype={0:str})

                #Create dataframe for data collection across whole table to store all values in desired format
                formatdf = pd.DataFrame(columns = ['date', 'flow_total'])
                
                #Loop around days
                for day in range(7):

                    #Record date from column header
                    survey_date = str(df.columns.values[day+1].date())

                    #df for specific column (date) in each table
                    day_df = pd.DataFrame(columns = ['date','flow_total'])

                    #create datetime
                    day_df['date'] = survey_date + " " + df['Unnamed: 0']
                    day_df['date']= pd.to_datetime(day_df['date'])

                    #Record flow value
                    day_df['flow_total'] = df[df.columns.values[day+1]]

                    #Replace NA corresponding to non-counts
                    nan_value = float("NaN")
                    day_df.replace("", nan_value, inplace=True)
                    day_df.dropna(subset = ["flow_total"], inplace=True)

                    #Append to main df for that table
                    formatdf = formatdf.append(day_df)
                
                #standardise direction naming
                if "Northeast" in dir:
                    formatdf["direction"] = "North East"

                elif "Northwest" in dir:
                    formatdf["direction"] = "North West"
                    
                elif "Southeast" in dir:
                    formatdf["direction"] = "South East"
                    
                elif "Southwest" in dir:
                    formatdf["direction"] = "South West"
                    
                elif "East" in dir:
                    formatdf["direction"] = "East"

                elif "West" in dir:
                    formatdf["direction"] = "West"
                    
                elif "North" in dir:
                    formatdf["direction"] = "North"
                    
                elif "South" in dir:
                    formatdf["direction"] = "South"
                elif "west" in dir:
                    formatdf["direction"] = "West"
                elif "east" in dir:
                    formatdf["direction"] = "East"
                
                #add remaining usused columns to dataframe
                formatdf['site'] = site
                formatdf['Source'] = 'ATC'
                formatdf['flow_car'] = ''
                formatdf['flow_LGV'] = ''
                formatdf['flow_OGV_1'] = ''
                formatdf['flow_OGV_2'] = ''
                formatdf['flow_HGV'] = ''

                survey_database = survey_database.append(formatdf)

    #add source field and output
    survey_database["Source"] = "HCC ATC"
    survey_database.to_csv(f"{output_dir}\\ATC_{year}_NC_2.csv", index = False)

def format_ATC_HCC_NC_3(input_dir: str, output_dir: str, discarded_dir: str, year: str):

    #define required variables
    os.chdir(input_dir)
    files = glob.glob("*.xlsx")
    discarded_data = pd.DataFrame()
    #final database:
    survey_database = pd.DataFrame()
    #time periods for non-count checks
    AM_PEAK_PERIOD = ["07:00", "8:00", "9:00"]
    PM_PEAK_PERIOD = ["16:00", "17:00", "18:00"]

    #loop through files, defining new file path each time
    for file in files:
        print("Adding ",file," to the database")
        file_path = os.path.join(input_dir,file)
        
        #retrieve site number
        sitedf = pd.read_excel(file_path,skiprows=0,usecols="A",index_col=None,nrows=0)
        site = str(sitedf.columns.values[0])
        site = site[-3:]
        
        #read up to a maximum of 100 tables in turn, skipping 177 rows for each one and reading the direction for each one.
        for table in range (100):
            skipr = 5+table*117
            dirdf = pd.read_excel(file_path,skiprows = skipr - 3,usecols="G",index_col=None,nrows=0)
            
            #if the cell including the direction is left blank, break the loop and move onto the next file
            if len(dirdf. columns) == 0:
                break
                
            #read direction from direction dataframe
            dir = dirdf.columns.values[0]
            
            #skip unneeded tables
            if dir == "Channel: Total Flow":
                    continue
            
            #read table of data
            df = pd.read_excel(file_path,skiprows = skipr ,nrows=96,dtype={0:str})
            #define datafame for data collection for entire table in the desired format
            formatdf = pd.DataFrame(columns = ['flow_total'])

            #for each day of the week...
            for day in range(7):
                
                #read the date off of the corresponding column
                survey_date = str(df.columns.values[day+1].date())

                #define new dataframe for each day
                day_df = pd.DataFrame(columns = ['date','flow_total'])
                #append date and time, assign to date column and convert to datetime object
                day_df['date'] = survey_date + " " + df['Begin']
                day_df['date']= pd.to_datetime(day_df['date'])

                #copy flows from the table for corresponding day
                day_df['flow_total'] = df[df.columns.values[day+1]]

                #add site and direction to the day dataframe
                day_df['site'] = site
                day_df['direction'] = dir

                #set datetime index so that .resample can be used to check for non-counts
                day_df = day_df.set_index("date")

                #define list of AM and PM peak datetime objects to check using resampled dataframe
                am_peak_datetime_objects = []
                for peak_hour in AM_PEAK_PERIOD:
                    peak_hour = survey_date + " " + peak_hour
                    peak_hour = datetime.datetime.strptime(peak_hour, '%Y-%m-%d %H:%M')
                    am_peak_datetime_objects.append(peak_hour)

                pm_peak_datetime_objects = []
                for peak_hour in PM_PEAK_PERIOD:
                    peak_hour = survey_date + " " + peak_hour
                    peak_hour = datetime.datetime.strptime(peak_hour, '%Y-%m-%d %H:%M')
                    pm_peak_datetime_objects.append(peak_hour)

                #lambda function defined to use in .resample(). Define resampled dataframe
                np_sum = lambda x: x.values.sum()
                hourly_data_table = day_df.resample('60min').agg({"site":"first","direction":"first","flow_total":np_sum})

                #set sum for AM peak period to zero before summing each peak period flow value
                period_sum = 0

                for peak_hour in am_peak_datetime_objects:
                    period_sum += hourly_data_table.loc[peak_hour, "flow_total"]
                    print(f"Time period {peak_hour} sum is: {period_sum}")
                
                #discard if total peak period flow is zero
                if period_sum == 0:
                    discard_record = hourly_data_table[["site", "direction"]].head(1)
                    discarded_data = pd.concat([discarded_data, discard_record], ignore_index = True)
                    continue
                
                #reset period sum to zero before checking PM peak period flow total
                period_sum = 0
                for peak_hour in pm_peak_datetime_objects:
                    period_sum += hourly_data_table.loc[peak_hour, "flow_total"]

                #discard if total peak period flow is zero
                if period_sum == 0:
                    discard_record = hourly_data_table[["site", "direction"]].head(1)
                    discarded_data = pd.concat([discarded_data, discard_record], ignore_index = True)
                    continue
                
                #reset index and re-label as date
                day_df = day_df.reset_index()
                day_df = day_df.rename(columns={"index":"date"})

                #add data for individual date to that holding data for the entire table
                formatdf = pd.concat([formatdf,day_df], ignore_index = True)

                #add remaining unused columns to dataframe
                formatdf['Source'] = 'ATC'
                formatdf['flow_car'] = ''
                formatdf['flow_LGV'] = ''
                formatdf['flow_OGV_1'] = ''
                formatdf['flow_OGV_2'] = ''
                formatdf['flow_HGV'] = ''

            #define NaN value to be replaced, corresponding to non-counts
            nan_value = float("NaN")
            #replace with blanks and delete corresponding rows
            formatdf.replace("", nan_value, inplace=True)
            formatdf.dropna(subset = ["flow_total"], inplace=True)

            #standardise direction naming
            if "Northeast" in dir:
                    formatdf["direction"] = "North East"

            elif "Northwest" in dir:
                    formatdf["direction"] = "North West"
                
            elif "Southeast" in dir:
                    formatdf["direction"] = "South East"
                
            elif "Southwest" in dir:
                    formatdf["direction"] = "South West"
                
                
            elif "East" in dir:
                    formatdf["direction"] = "East"

            elif "West" in dir:
                    formatdf["direction"] = "West"
                
            elif "North" in dir:
                    formatdf["direction"] = "North"
                
            elif "South" in dir:
                    formatdf["direction"] = "South"

            #Add onto final database
            survey_database = pd.concat([survey_database,formatdf], ignore_index = True)
    
    #rename source field and output
    survey_database["Source"] = "HCC ATC"
    survey_database.to_csv(f"{output_dir}\\ATC_{year}_HCC_NC_3.csv", index = False)
    discarded_data.to_csv(f"{discarded_dir}\\Discarded_Data_ATC_{year}_HCC_NC_3.csv", index = False)

def format_ATC_C_1_1(input_dir: str, output_dir: str, year: int, discarded_dir: str):
    
    #define required variables
    os.chdir(input_dir)
    files = glob.glob("*.csv")
    discarded_data = pd.DataFrame()
    #final database:
    survey_database = pd.DataFrame()
    #time periods for non-count checks
    AM_PEAK_PERIOD = ["07:00", "08:00", "09:00"]
    PM_PEAK_PERIOD = ["16:00", "17:00", "18:00"]

    #loop through files
    for file in files:
        print("Adding ",file," to the database")
        
        #define table counter
        table = 0
        
        #read file 117 rows at a time, looping through tables
        for data in pd.read_csv(file, chunksize = 117):
            #increment table count by 1
            table += 1

            #read site number off of first row in the file
            site = str(data.columns.values[0])[-3:]
            print ("Table number : ", table, "   Site number :  ", site)

            #read direction for each table
            dir = data.iloc[1,6][9:]
            
            #read date for each table
            date = str(data.iloc[1,3])
            
            #skip total flow tables
            if dir == "Total Flow":
                continue
            
            #reset data to correspond to flow data table, including column names
            data = data.rename(columns = data.iloc[4])
            data = data[5:101]
            #convert to numerical values to avoid string error
            data[['Car/lVan', 'Cr/lV+Tr', 'H. Van','LGV', 'Rigid', 'Rg+Tr', 'ArticHGV', 'Minibus', 'Bus' ]] = data[['Car/lVan', 'Cr/lV+Tr', 'H. Van','LGV', 'Rigid', 'Rg+Tr', 'ArticHGV', 'Minibus', 'Bus' ]].apply(pd.to_numeric)
            
            #define 'Date' column containing just the date, and append to the time for each row before converting to datetime object in 'date' column
            data['Date'] = date
            data['date'] = data['Date'] + " " + data['Begin']
            data['date'] = pd.to_datetime(data['date'])
            
            #standardise direction naming
            if "East" in dir:
                data["direction"] = "East"

            elif "West" in dir:
                data["direction"] = "West"
            
            elif "North" in dir:
                data["direction"] = "North"
            
            elif "South" in dir:
                data["direction"] = "South"

            data['site'] = site

            #aggregate flow values into different user classes and define flow total. Replace blanks with zeros
            data['flow_car'] = data['Car/lVan'] + data['Cr/lV+Tr']
            data['flow_LGV'] = data['H. Van'] + data['LGV']
            data['flow_OGV_1'] = data['Rigid'] 
            data['flow_OGV_2'] = data['Rg+Tr'] + data['ArticHGV']
            data['flow_HGV'] = data['flow_OGV_1'] + data['flow_OGV_2']
            data['flow_BUS'] = data['Minibus'] + data['Bus']
            data['flow_total'] = data['flow_car'] + data['flow_LGV'] +  data['flow_HGV'] + data['flow_BUS']
            data[["flow_car", "flow_LGV", "flow_OGV_1", "flow_OGV_2", "flow_HGV", "flow_BUS", "flow_total"]] = data[["flow_car", "flow_LGV", "flow_OGV_1", "flow_OGV_2", "flow_HGV", "flow_BUS", "flow_total"]].fillna(0)
            
            #define list of AM and PM datetime objects to check on the dataframe
            am_peak_datetime_objects = []
            for peak_hour in AM_PEAK_PERIOD:
                peak_hour = date + " " + peak_hour
                peak_hour = datetime.datetime.strptime(peak_hour, '%d-%b-%y %H:%M')
                am_peak_datetime_objects.append(peak_hour)

            pm_peak_datetime_objects = []
            for peak_hour in PM_PEAK_PERIOD:
                peak_hour = date + " " + peak_hour
                peak_hour = datetime.datetime.strptime(peak_hour, '%d-%b-%y %H:%M')
                pm_peak_datetime_objects.append(peak_hour)
            
            #set datetime index so that .resample() can be used to check for non counts
            data = data.set_index('date')

            #define resampled table using lambda sum function to check for non counts
            np_sum = lambda x: x.values.sum()
            hourly_data_table = data.resample('60min').agg({"site":"first","direction":"first","flow_car":np_sum,"flow_LGV":np_sum,"flow_OGV_1":np_sum,"flow_OGV_2":np_sum,"flow_HGV":np_sum,"flow_BUS":np_sum,"flow_total":np_sum})
            hourly_data_table["flow_total"] = hourly_data_table["flow_total"].fillna(0)

            #set sum for AM peak period to zero before summing each peak period flow value
            period_sum = 0

            for peak_hour in am_peak_datetime_objects:
                period_sum += hourly_data_table.loc[peak_hour, "flow_total"]
                print(f"Time period {peak_hour} sum is: {period_sum}")

            #if zero flow in AM peak, skip table and add to discarded data
            if period_sum == 0:
                discard_record = hourly_data_table[["site", "direction"]].head(1)
                discarded_data = pd.concat([discarded_data, discard_record])
                continue
            
            #reset period sum to zero before checking PM peak period flow total
            period_sum = 0

            for peak_hour in pm_peak_datetime_objects:
                period_sum += hourly_data_table.loc[peak_hour, "flow_total"]

            #discard if total peak period flow is zero
            if period_sum == 0:
                discard_record = hourly_data_table[["site", "direction"]].head(1)
                discarded_data = pd.concat([discarded_data, discard_record])
                continue
            
            #reset index and rename index column to date
            data = data.reset_index()
            data = data.rename(columns = {"index": "date"})
            
            #drop unneeded columns
            data = data.drop(data.columns[[1,2,3,4,5,6,7,8,9,10,11,12,13]],axis = 1)

            #append resulting dataframe to main dataframe
            survey_database = survey_database.append(data)
    
    #rename source field and output
    survey_database["Source"] = "HCC ATC"
    survey_database.to_csv(f"{output_dir}\\ATC_{str(year)}_HCC_C_1_1.csv", index = False)
    discarded_data.to_csv(f"{discarded_dir}\\ATC_{str(year)}_HCC_C_1_1_Discarded_Data.csv", index=False)

def format_ATC_HCC_C_1_2(input_dir: str, output_dir: str, discarded_dir: str, year: int):
    
    #define required variables
    os.chdir(input_dir)
    files = glob.glob("*.csv")
    discarded_data = pd.DataFrame()
    #main dataframe
    survey_database = pd.DataFrame()
    #time periods for non-counts
    AM_PEAK_PERIOD = ["07:00", "8:00", "9:00"]
    PM_PEAK_PERIOD = ["16:00", "17:00", "18:00"]

    #loop through files
    for file in files:
        print("Adding ",file," to the database")

        #define table counter
        table = 0
        

        #read file 117 rows at a time, looping through tables
        for data in pd.read_csv(file, chunksize = 117):
            #increment table count by 1
            table += 1

            #read site number off of first row in the file
            site = str(data.columns.values[0])[-3:]
            print ("Table number : ", table, "   Site number :  ", site)

            #fix site number error on site 611
            if site == "001":
                    site = "611"
            else:
                    site = site[-3:]


            #read direction for each table
            dir = data.iloc[1,6][9:]
            
            #read date for each table
            date = str(data.iloc[1,3])
            
            #choose specific direction names to add to main database
            if dir == "North" or dir == "South" or dir == "East" or dir =="West" or dir == "Northbound " or dir == "Southbound " or dir == "Eastbound " or dir == "Westbound ":
                
                
                #redefine dataset to correspond to table of flow values
                data = data.rename(columns = data.iloc[4])
                data = data[5:101]
                #convert to numerical values to avoid string error
                data[['Car/lVan', 'Cr/lV+Tr', 'H. Van','LGV', 'Rigid', 'Rg+Tr', 'ArticHGV', 'Minibus', 'Bus' ]] = data[['Car/lVan', 'Cr/lV+Tr', 'H. Van','LGV', 'Rigid', 'Rg+Tr', 'ArticHGV', 'Minibus', 'Bus' ]].apply(pd.to_numeric)
                
                #define 'Date' column containing just the date, append with time and convert to datetime object in 'date' column
                data['Date'] = date
                data['date'] = data['Date'] + " " + data['Begin']
                data['date'] = pd.to_datetime(data['date'])
                
                #create columns with direction name and site number
                data['direction'] = dir
                data['site'] = site

                #create lists of datetime objects in AM and PM peak periods used for non-count removal
                am_peak_datetime_objects = []
                for peak_hour in AM_PEAK_PERIOD:
                    peak_hour = date + " " + peak_hour
                    peak_hour = datetime.datetime.strptime(peak_hour, '%d-%b-%y %H:%M')
                    am_peak_datetime_objects.append(peak_hour)

                        
                pm_peak_datetime_objects = []
                for peak_hour in PM_PEAK_PERIOD:
                    peak_hour = date + " " + peak_hour
                    peak_hour = datetime.datetime.strptime(peak_hour, '%d-%b-%y %H:%M')
                    pm_peak_datetime_objects.append(peak_hour)

                #aggregate flow values into different user classes and define flow total. replace blanks with zeros
                data['flow_car'] = data['Car/lVan'] + data['Cr/lV+Tr']
                data['flow_LGV'] = data['H. Van'] + data['LGV']
                data['flow_OGV_1'] = data['Rigid'] 
                data['flow_OGV_2'] = data['Rg+Tr'] + data['ArticHGV']
                data['flow_HGV'] = data['flow_OGV_1'] + data['flow_OGV_2']
                data['flow_BUS'] = data['Minibus'] + data['Bus']
                data['flow_total'] = data['flow_car'] + data['flow_LGV'] +  data['flow_HGV'] + data['flow_BUS']
                
                #drop unneeded columns
                data = data.drop(data.columns[[0,1,2,3,4,5,6,7,8,9,10,11,12,]],axis = 1)

                #set datetime index for resample
                data = data.set_index(["date"])

                #define lambda sum and resample
                np_sum = lambda x: x.values.sum()
                hourlydf = data.resample('60min').agg({"site":"first","direction":"first","flow_car":np_sum,"flow_LGV":np_sum,"flow_OGV_1":np_sum,"flow_OGV_2":np_sum,"flow_HGV":np_sum,"flow_BUS":np_sum,"flow_total":np_sum})

                #check for non-counts and remove
                period_sum = 0
                    
                for peak_hour in am_peak_datetime_objects:
                        period_sum += hourlydf.loc[peak_hour, "flow_total"]
                    
                if period_sum == 0:
                        discard_record = hourlydf[["site", "direction"]].head(1)
                        discarded_data = pd.concat([discarded_data, discard_record], ignore_index = True)
                        table += 1
                        continue

                period_sum = 0
                for peak_hour in pm_peak_datetime_objects:
                        period_sum += hourlydf.loc[peak_hour, "flow_total"]

                    
                if period_sum == 0:
                        discard_record = hourlydf[["site", "direction"]].head(1)
                        discarded_data = pd.concat([discarded_data, discard_record], ignore_index = True)
                        table += 1
                        continue
                
                #replace NaN values with blanks and remove rows
                nan_value = float("NaN")
                data.replace("", nan_value, inplace=True)
                data.dropna(subset = ["flow_total"], inplace=True)            

                #define source column
                data['Source'] = 'HCC ATC'


                #standardise direction naming
                if "Northeast" in dir:
                        data["direction"] = "North East"

                elif "Northwest" in dir:
                        data["direction"] = "North West"
                    
                elif "Southeast" in dir:
                        data["direction"] = "South East"
                    
                elif "Southwest" in dir:
                        data["direction"] = "South West"
                    
                elif "East" in dir:
                        data["direction"] = "East"

                elif "West" in dir:
                        data["direction"] = "West"
                    
                elif "North" in dir:
                    data["direction"] = "North"
                    
                elif "South" in dir:
                    data["direction"] = "South"

                #reset and relabel datetime index
                data = data.reset_index()
                data = data.rename(columns = {"index": "date"})

                #add to final database
                survey_database = pd.concat([survey_database,data], ignore_index = True)
    #drop unneeded Date column, define Source field and output
    survey_database = survey_database.drop(columns = ["Date"])
    survey_database["Source"] = "HCC ATC"
    
    survey_database.to_csv(f"{output_dir}\\ATC_{str(year)}_HCC_C_1_2.csv", index = False)
    discarded_data.to_csv(f"{discarded_dir}\\Discarded_Data_ATC_{str(year)}_HCC_C_1_2.csv", index= False)

#### START TIDYING HERE


def format_ATC_2022_C_2_1(input_dir: str, output_dir: str, discarded_dir: str):

    #INPUTS
    #This constant should be set to the number of cells each table and its header takes up i.e how many cells before it repeats style
    ROWS_TO_SKIP = 118
    
    AM_PEAK_PERIOD = ["07:00", "08:00", "09:00"]
    PM_PEAK_PERIOD = ["16:00", "17:00", "18:00"]

    ########################################################################################################################################################################

    #PROCESS

    os.chdir(input_dir)

    #Retrieves all Excel workbook files within the folder
    file_names = glob.glob("*.xlsx")

    appended_data = pd.DataFrame()
    discarded_data = pd.DataFrame()

    #loop around files
    for file in file_names:
        print("Added", file, "to the database")
        
        #Open Excel file to extract sheet names
        sheets = pd.ExcelFile(file)
        sheet_names = sheets.sheet_names

        #Close to increase efficiency
        sheets.close()

        sheet_number = 1

        #Iteration over each workbook sheets (Each referes to a different survey date)
        for name in sheet_names:   

            print(f"Processing sheet {sheet_number}")
            
            data = pd.read_excel(file, sheet_name = name)
            
            #The direction name 'Total flow' is used to indicate a sheet has finished being read unless the sheet is an exception
            last_direction_name = "Total Flow"
            
            #Checks to see of the last direction name is as expected and if not sets it to the value that is there (coordinates taken from sheet itself)
            if str(data.iloc[-114,-2]) != "Total Flow":
                last_direction_name = str(data.iloc[-114,-2])

            #Site consistent across each individual workbook and only want the last 3 digits
            site = str(data.columns.values[1])

            #Catching bug in one ATC name
            if site == "78000001":
                site = "611"
            else:
                site = site[-3:]

            finished_reading = False
            table_number = 1

            #Iteration over each table on a sheet (each table corresponds to a ATC count for a direction)
            #Note: Sometimes multiple for one direction if there are multiple lanes
            while not finished_reading:
                
                print(f"Processing Table {table_number}")
                
                if table_number == 1:
                    #Coordinates may need tweaking if format changes. Note: position relative to right hand side of dataframe as the width of the table may change
                    date = str(data.iloc[1,-7])
                    direction = str(data.iloc[1,-2])

                    #Used to remove the header from the data after important info is extracted
                    data = data[4:]

                else:
                    #4 is subtracted to account for the removal of the header from the last iteration
                    data = data[(ROWS_TO_SKIP-4):]
                    date = str(data.iloc[1,-7])
                    direction = str(data.iloc[1,-2])
                    data = data[4:]
                
                #Removes unwanted tables or tables that are often later sumarised by another table
                if direction[-1].isdigit() or (" OS" in direction) or (" NS" in direction) or ("Cycle" in direction):
                    table_number += 1
                    continue
                
                #Removes tables from directions that include two compass diresctions as these are often sumarised by another table
                elif ("South" in direction and (("West" in direction) or ("East" in direction ))) or ("North" in direction and (("West" in direction) or ("East" in direction ))): 
                    table_number += 1
                    continue
                
                #If the end has been reached and the table is a 'Total Flow' table then no data needs to be taken and the next sheet can be read
                elif (last_direction_name in direction) and (last_direction_name == "Total Flow"):
                    finished_reading = True
                    break
                
                #In the case where the end has been reached but it is not a 'Total Flow' table the data is still required and the next sheet is needed
                elif last_direction_name in direction:
                    finished_reading = True
                
                #Only the relevant data is taken for further calculation
                data_table = data[:97]

                #Set the column names so they are relevant to the data being processed and then remove the existing data headings
                data_table.columns = data_table.iloc[0]
                data_table.columns.values[0] = "time"
                data_table = data_table[1:]

                #If a table has been left blank no processing can occur so it is skipped
                if np.isnan(data_table.iloc[1,1]):
                    table_number += 1
                    continue
                
                #define site, direction, and datetime columns
                data_table['direction'] = direction
                data_table['site'] = site
                data_table['Date'] = date
                data_table["time"] = data_table['time'].astype(str)
                data_table["date"] = data_table["Date"] + " " + data_table["time"]
                data_table['date'] = pd.to_datetime(data_table['date'])

                #prepare check for non-counts
                am_peak_datetime_objects = []
                for peak_hour in AM_PEAK_PERIOD:
                    peak_hour = date + " " + peak_hour
                    peak_hour = datetime.datetime.strptime(peak_hour, '%d %B %Y %H:%M')
                    am_peak_datetime_objects.append(peak_hour)

                pm_peak_datetime_objects = []
                for peak_hour in PM_PEAK_PERIOD:
                    peak_hour = date + " " + peak_hour
                    peak_hour = datetime.datetime.strptime(peak_hour, '%d %B %Y %H:%M')
                    pm_peak_datetime_objects.append(peak_hour)

                #Calculations and aggregation
                data_table['flow_car'] = data_table['Bin 2\nCar/lVan'] + data_table['Bin 3\nCr/lV+Tr']
                data_table['flow_LGV'] = data_table['Bin 4\nH. Van'] + data_table['Bin 5\nLGV']
                data_table['flow_OGV_1'] = data_table['Bin 6\nRigid']
                data_table['flow_OGV_2'] = data_table['Bin 7\nRg+Tr'] + data_table['Bin 8\nArticHGV']
                data_table['flow_HGV'] = data_table['flow_OGV_1'] + data_table['flow_OGV_2']
                data_table['flow_BUS'] = data_table['Bin 9\nMinibus'] + data_table['Bin 10\nBus']
                data_table['flow_total'] = data_table['flow_car'] + data_table['flow_LGV'] + data_table['flow_HGV'] + data_table['flow_BUS']
                data_table[["flow_car", "flow_LGV", "flow_OGV_1", "flow_OGV_2", "flow_HGV", "flow_BUS", "flow_total"]] = data_table[["flow_car", "flow_LGV", "flow_OGV_1", "flow_OGV_2", "flow_HGV", "flow_BUS", "flow_total"]].fillna(0)
                
                #set datetime index for .resample()
                data_table = data_table.set_index("date")
                #drop unneeded columns
                data_table = data_table.drop(columns = ["Bin 2\nCar/lVan", "Bin 3\nCr/lV+Tr", "Bin 4\nH. Van", "Bin 5\nLGV", "Bin 6\nRigid", "Bin 7\nRg+Tr", "Bin 8\nArticHGV", "Bin 9\nMinibus", "Bin 10\nBus", "Bin 1\nM/Cycle", "Date", "time", "Total\nVolume"])
                
                #Removes cycling data from relevant tables
                if len(data_table.columns) == 10:
                    data_table = data_table.drop(columns = ["Bin 11\nCycle"])

                #We do not want to keep this version of the data table it is just to check AM and PM peak periods in case they are empty
                np_sum = lambda x: x.values.sum()
                hourly_data_table = data_table.resample('60min').agg({"site":"first","direction":"first","flow_car":np_sum,"flow_LGV":np_sum,"flow_OGV_1":np_sum,"flow_OGV_2":np_sum,"flow_HGV":np_sum,"flow_BUS":np_sum,"flow_total":np_sum})

                #check for non-counts and discard
                period_sum = 0
                
                for peak_hour in am_peak_datetime_objects:
                    period_sum += hourly_data_table.loc[peak_hour, "flow_total"]
                
                if period_sum == 0:
                    discard_record = hourly_data_table[["site", "direction"]].head(1)
                    discarded_data = pd.concat([discarded_data, discard_record])
                    table_number += 1
                    continue

                period_sum = 0
                for peak_hour in pm_peak_datetime_objects:
                    period_sum += hourly_data_table.loc[peak_hour, "flow_total"]

                if period_sum == 0:
                    discard_record = hourly_data_table[["site", "direction"]].head(1)
                    discarded_data = pd.concat([discarded_data, discard_record])
                    table_number += 1
                    continue

                #Converts directions to uniform style
                if "East" in direction:
                    data_table["direction"] = "East"

                elif "West" in direction:
                    data_table["direction"] = "West"
                
                elif "North" in direction:
                    data_table["direction"] = "North"
                
                elif "South" in direction:
                    data_table["direction"] = "South"
                
                #Adds new data to the end of compiled data after recreating datetime column

                data_table = data_table.reset_index()
                data_table = data_table.rename(columns = {"index": "date"})
                appended_data = pd.concat([appended_data, data_table])
                table_number += 1
            sheet_number += 1

    #define source field and output
    appended_data["Source"] = "HCC ATC"
    appended_data.to_csv(f"{output_dir}\\ATC_2022_C_2_1.csv", index = False)
    discarded_data.to_csv(f"{discarded_dir}\\ATC_2022_C_2_1_Discarded_Data.csv", index=False)


def format_ATC_2022_C_2_2(input_dir: str, output_dir: str, discarded_dir: str):

    #INPUTS
    #This constant should be set to the number of cells each table and its header takes up i.e how many cells before it repeats style
    ROWS_TO_SKIP = 118
    AM_PEAK_PERIOD = ["07:00", "8:00", "9:00"]
    PM_PEAK_PERIOD = ["16:00", "17:00", "18:00"]

    #PROCESS

    os.chdir(input_dir)

    #Retrieves all Excel workbook files within the folder
    file_names = glob.glob("*.xlsx")

    appended_data = pd.DataFrame()
    discarded_data = pd.DataFrame()

    for file in file_names:
        print("Added", file, "to the database")
        
        
        #Open Excel file to extract sheet names
        sheets = pd.ExcelFile(file)
        sheet_names = sheets.sheet_names

        #Close to increase efficiency
        sheets.close()

        sheet_number = 1

        #Iteration over each workbook sheets (Each referes to a different survey date)
        for name in sheet_names:   

            print(f"Processing sheet {sheet_number}")
            
            data = pd.read_excel(file, sheet_name = name)
            
            #The direction name 'Total flow' is used to indicate a sheet has finished being read unless the sheet is an exception
            last_direction_name = "Total Flow"
            
            #Checks to see of the last direction name is as expected and if not sets it to the value that is there (coordinates taken from sheet itself)
            if str(data.iloc[-114,-2]) != "Total Flow":
                last_direction_name = str(data.iloc[-114,-2])

            #Site consistent across each individual workbook
            site = str(data.columns.values[1])
            
            #Catching bug in one ATC name
            if site == "78000001":
                site = "611"
            else:
                site = site[-3:]

            finished_reading = False
            table_number = 1

            #Iteration over each table on a sheet (each table corresponds to a ATC count for a direction)
            #Note: Sometimes multiple for one direction if there are multiple lanes
            while not finished_reading:
                
                print(f"Processing Table {table_number}")
                
                if table_number == 1:
                    #Coordinates may need tweaking if format changes. Note: position relative to right hand side of dataframe as the width of the table may change
                    date = str(data.iloc[1,-7])
                    direction = str(data.iloc[1,-2])

                    #Used to remove the header from the data after important info is extracted
                    data = data[4:]

                else:
                    #4 is subtracted to account for the removal of the header from the last iteration
                    data = data[(ROWS_TO_SKIP-4):]
                    date = str(data.iloc[1,-7])
                    direction = str(data.iloc[1,-2])
                    data = data[4:]

                #If the end has been reached and the table is a 'Total Flow' table then no data needs to be taken and the next sheet can be read
                if (last_direction_name in direction) and (last_direction_name == "Total Flow"):
                    finished_reading = True
                    break
                
                #In the case where the end has been reached but it is not a 'Total Flow' table the data is still required and the next sheet is needed
                elif last_direction_name in direction:
                    finished_reading = True

                #Removes unwanted tables or tables that are often later sumarised by another table
                elif not (direction[-1].isdigit() or ("South" in direction and (("West" in direction) or ("East" in direction ))) or ("North" in direction and (("West" in direction) or ("East" in direction )))):
                    table_number += 1
                    continue
                

                
                #Only the relevant data is taken for further calculation
                data_table = data[:97]

                #Set the column names so they are relevant to the data being processed and then remove the existing data headings
                data_table.columns = data_table.iloc[0]
                data_table.columns.values[0] = "time"
                data_table = data_table[1:]

                #If a table has been left blank no processing can occur so it is skipped
                if np.isnan(data_table.iloc[1,1]):
                    table_number += 1
                    continue
                
                #data_table['direction'] = direction
                data_table['site'] = site

                data_table['Date'] = date
                data_table["time"] = data_table['time'].astype(str)
                data_table["date"] = data_table["Date"] + " " + data_table["time"]
                data_table['date'] = pd.to_datetime(data_table['date'], dayfirst = True)

                if "East" in direction:
                    data_table["direction"] = "East"

                elif "West" in direction:
                    data_table["direction"] = "West"
                
                elif "North" in direction:
                    data_table["direction"] = "North"
                
                elif "South" in direction:
                    data_table["direction"] = "South"

                am_peak_datetime_objects = []
                for peak_hour in AM_PEAK_PERIOD:
                    peak_hour = date + " " + peak_hour
                    peak_hour = datetime.datetime.strptime(peak_hour, '%d %B %Y %H:%M')
                    am_peak_datetime_objects.append(peak_hour)

                pm_peak_datetime_objects = []
                for peak_hour in PM_PEAK_PERIOD:
                    peak_hour = date + " " + peak_hour
                    peak_hour = datetime.datetime.strptime(peak_hour, '%d %B %Y %H:%M')
                    pm_peak_datetime_objects.append(peak_hour)

                #Calculations and aggregation
                data_table['flow_car'] = data_table['Bin 2\nCar/lVan'] + data_table['Bin 3\nCr/lV+Tr']
                data_table['flow_LGV'] = data_table['Bin 4\nH. Van'] + data_table['Bin 5\nLGV']
                data_table['flow_OGV_1'] = data_table['Bin 6\nRigid']
                data_table['flow_OGV_2'] = data_table['Bin 7\nRg+Tr'] + data_table['Bin 8\nArticHGV']
                data_table['flow_HGV'] = data_table['flow_OGV_1'] + data_table['flow_OGV_2']
                data_table['flow_BUS'] = data_table['Bin 9\nMinibus'] + data_table['Bin 10\nBus']
                data_table['flow_total'] = data_table['flow_car'] + data_table['flow_LGV'] + data_table['flow_HGV'] + data_table['flow_BUS']
                data_table[["flow_car", "flow_LGV", "flow_OGV_1", "flow_OGV_2", "flow_HGV", "flow_BUS", "flow_total"]] = data_table[["flow_car", "flow_LGV", "flow_OGV_1", "flow_OGV_2", "flow_HGV", "flow_BUS", "flow_total"]].fillna(0)
                data_table = data_table.set_index("date")

                data_table = data_table.drop(columns = ["Bin 2\nCar/lVan", "Bin 3\nCr/lV+Tr", "Bin 4\nH. Van", "Bin 5\nLGV", "Bin 6\nRigid", "Bin 7\nRg+Tr", "Bin 8\nArticHGV", "Bin 9\nMinibus", "Bin 10\nBus", "Bin 1\nM/Cycle", "Date", "time", "Total\nVolume"])
                
                #Removes cycling data from relevant tables
                if len(data_table.columns) == 10:
                    data_table = data_table.drop(columns = ["Bin 11\nCycle"])
                
                #We do not want to keep this version of the data table it is just to check AM and PM peak periods in case they are empty
                np_sum = lambda x: x.values.sum()
                hourly_data_table = data_table.resample('60min').agg({"site":"first","direction":"first","flow_car":np_sum,"flow_LGV":np_sum,"flow_OGV_1":np_sum,"flow_OGV_2":np_sum,"flow_HGV":np_sum,"flow_BUS":np_sum,"flow_total":np_sum})

                period_sum = 0
                
                for peak_hour in am_peak_datetime_objects:
                    period_sum += hourly_data_table.loc[peak_hour, "flow_total"]
                
                if period_sum == 0:
                    discard_record = hourly_data_table[["site", "direction"]].head(1)
                    discarded_data = pd.concat([discarded_data, discard_record])
                    table_number += 1
                    continue

                period_sum = 0
                for peak_hour in pm_peak_datetime_objects:
                    period_sum += hourly_data_table.loc[peak_hour, "flow_total"]

                if period_sum == 0:
                    discard_record = hourly_data_table[["site", "direction"]].head(1)
                    discarded_data = pd.concat([discarded_data, discard_record])
                    table_number += 1
                    continue

                #Converts directions to uniform style
                #if (("South" in direction) and (("West" in direction) or ("East" in direction))) or (("North" in direction) and (("West" in direction) or ("East" in direction))):
                    #print("Multi direction")
                
                
                data_table = data_table.reset_index()
                data_table = data_table.rename(columns = {"index": "date"})
                data_table[["flow_car", "flow_LGV", "flow_OGV_1", "flow_OGV_2", "flow_HGV", "flow_BUS", "flow_total"]] = data_table[["flow_car", "flow_LGV", "flow_OGV_1", "flow_OGV_2", "flow_HGV", "flow_BUS", "flow_total"]].fillna(0)
                #Adds new data to the end of compiled data
                appended_data = pd.concat([appended_data, data_table])
                table_number += 1
            sheet_number += 1
    appended_data["Source"] = "HCC ATC"
    appended_data.to_csv(f"{output_dir}\\ATC_2022_C_2_2.csv", index = False)
    discarded_data.to_csv(f"{discarded_dir}\\ATC_2022_C_2_2_Discarded_Data.csv", index=False)



def format_ATC_2023_SEVERNSIDE(input_dir: str, output_dir: str):

    #set working directory
    os.chdir(input_dir)
    #load in dataframe of required time periods (not including 5 minute intervals present in the .xlsm files)
    time_filter = pd.read_csv("Time_Filter.csv")
    time_filter['TIME'] = time_filter['TIME'].astype(str)

    #list files
    file_names = glob.glob("*.xlsm")

    #define AM and PM peak periods for non-count check
    AM_PEAK_PERIOD = ["07:00", "08:00", "09:00"]
    PM_PEAK_PERIOD = ["16:00", "17:00", "18:00"]

    #define output dataframe with required column names, and discarded dataframe
    appended_data = pd.DataFrame(columns = ["date","site", "direction", 'flow_car', 'flow_LGV', 'flow_OGV_1', "flow_OGV_2", "flow_HGV",'flow_BUS', 'flow_total'])
    discarded_data = pd.DataFrame()

    print(file_names)

    #loop through files
    for file in file_names:
        print("Adding ", file, " to the database")

        #read site number from file name
        site =  re.findall('\s[0-9]+', file)[0][1:]
        print(site)

        #define list of sheet names
        sheets = pd.ExcelFile(file)
        sheet_names = sheets.sheet_names
        sheets.close()

        sheet_number = 1

        #loop through sheet names
        for name in sheet_names:

            print(f"Processing sheet {name}")

            #skip unneeded sheets
            if(name[2] != " "):
                continue
            if(name[3] == "m"):
                continue
            
            #define date from sheet name and reformat
            date = name
            date = date.replace(" ",r"/")
            print(date)

            #create two dataframes of desired format to read data from the tables corresponding to each direction
            temp_df = pd.DataFrame(columns = ["date","site", "direction", 'flow_car', 'flow_LGV', 'flow_OGV_1', "flow_OGV_2",'flow_BUS', "flow_HGV", "flow_bicycle", "flow_motorcycle", 'flow_total', "average_speed_mph"])
            temp_df2 = pd.DataFrame(columns = ["date","site", "direction", 'flow_car', 'flow_LGV', 'flow_OGV_1', "flow_OGV_2",'flow_BUS', "flow_HGV", "flow_bicycle", "flow_motorcycle", 'flow_total', "average_speed_mph"])

            #read in excel file
            data = pd.read_excel(file, sheet_name = name)

            #read names of each direction and redefine dataframe to correspond to location of data within file
            dir1 = data.iloc[6,1][14:]
            dir2 = data.iloc[6,13][14:]
            data.columns = data.iloc[7]
            data = data[8:294]

            #convert TIME column to string and filter unneeded time periods
            data["TIME"] = data["TIME"].astype(str)
            data = pd.merge(data,time_filter, how = "inner", on = "TIME")

            #define datetime and other data columns in df holding resformatted data from left table
            temp_df['date'] = date + " " + data["TIME"]
            temp_df['date'] = pd.to_datetime(temp_df['date'], dayfirst = True)
            temp_df["site"] = site
            temp_df["direction"] = dir1
            temp_df.iloc[:,[3,4,5,6,7,9,10,12]] = data.iloc[:,[1,2,3,4,5,7,6,8]]
            temp_df["flow_HGV"] = temp_df['flow_OGV_1'] + temp_df['flow_OGV_2']
            temp_df["flow_total"] = temp_df['flow_car'] + temp_df['flow_LGV'] + temp_df['flow_HGV'] + temp_df['flow_BUS']# + temp_df["flow_bicycle"] + temp_df["flow_motorcycle"]
            
            #define lists of AM and PM peak period times used for non-count check
            am_peak_datetime_objects = []
            for peak_hour in AM_PEAK_PERIOD:
                    peak_hour = date + " " + peak_hour
                    peak_hour = datetime.datetime.strptime(peak_hour, '%d/%m/%Y %H:%M')
                    am_peak_datetime_objects.append(peak_hour)
                    
            pm_peak_datetime_objects = []
            for peak_hour in PM_PEAK_PERIOD:
                    peak_hour = date + " " + peak_hour
                    peak_hour = datetime.datetime.strptime(peak_hour, '%d/%m/%Y %H:%M')
                    pm_peak_datetime_objects.append(peak_hour)

            #set datetime index so that .resample can be used
            temp_df = temp_df.set_index(["date"])
            
            #define lambda sum and resample. use lambda mean for avg speed
            np_sum = lambda x: x.values.sum()
            #np_avg = lambda x: x.values.mean()
            hourlydf = temp_df.resample('60min').agg({"site":"first","direction":"first","flow_car":np_sum,"flow_LGV":np_sum,"flow_OGV_1":np_sum,"flow_OGV_2":np_sum,"flow_HGV":np_sum,"flow_BUS":np_sum,"flow_bicycle":np_sum,"flow_motorcycle": np_sum, "flow_total":np_sum, "average_speed_mph": "first"})

            #remove non-counts####
            period_sum = 0
            
            delete_day = False
            
            for peak_hour in am_peak_datetime_objects:
                    period_sum += hourlydf.loc[peak_hour, "flow_total"]
                
            if period_sum == 0:
                    delete_day = True
                    discard_record = hourlydf[["site", "direction"]].head(1)
                    discarded_data = pd.concat([discarded_data, discard_record])
                    

            period_sum = 0
            for peak_hour in pm_peak_datetime_objects:
                    period_sum += hourlydf.loc[peak_hour, "flow_total"]

                
            if period_sum == 0:
                    delete_day = True
                    discard_record = hourlydf[["site", "direction"]].head(1)
                    discarded_data = pd.concat([discarded_data, discard_record])
            
            ######################

            #convert NaN to strings and delete corresponding rows
            nan_value = float("NaN")
            if delete_day == False:
                temp_df.replace("", nan_value, inplace=True)
                
                temp_df.dropna(subset = ["flow_total"], inplace=True)

                #reset and relabel index to datetime column
                temp_df = temp_df.reset_index()
                temp_df = temp_df.rename(columns = {"index": "date"})

                #append to final database
                appended_data = pd.concat([appended_data, temp_df])

            #define datetime and other required columns in df holding resformatted data from right table
            temp_df2['date'] = date + " " + data["TIME"]
            temp_df2['date'] = pd.to_datetime(temp_df2['date'], dayfirst = True)
            temp_df2["site"] = site
            temp_df2["direction"] = dir2
            temp_df2.iloc[:,[3,4,5,6,7,9,10,12]] = data.iloc[:,[13,14,15,16,17,18,19,20]]
            temp_df2["flow_HGV"] = temp_df2['flow_OGV_1'] + temp_df2['flow_OGV_2']
            temp_df2["flow_total"] = temp_df2['flow_car'] + temp_df2['flow_LGV'] + temp_df2['flow_HGV'] + temp_df2['flow_BUS']# + temp_df2["flow_bicycle"] + temp_df2["flow_motorcycle"]

            #define lists of AM and PM peak period times used for non-count check for second dimension
            am_peak_datetime_objects = []
            for peak_hour in AM_PEAK_PERIOD:
                    peak_hour = date + " " + peak_hour
                    peak_hour = datetime.datetime.strptime(peak_hour, '%d/%m/%Y %H:%M')
                    am_peak_datetime_objects.append(peak_hour)

                    
            pm_peak_datetime_objects = []
            for peak_hour in PM_PEAK_PERIOD:
                    peak_hour = date + " " + peak_hour
                    peak_hour = datetime.datetime.strptime(peak_hour, '%d/%m/%Y %H:%M')
                    pm_peak_datetime_objects.append(peak_hour)

            #set index of second table to datetime for resample
            temp_df2 = temp_df2.set_index(["date"])

            #resample for non-count check
            np_sum2 = lambda x: x.values.sum()
            np_avg2 = lambda x: x.values.mean()
            hourlydf2 = temp_df2.resample('60min').agg({"site":"first","direction":"first","flow_car":np_sum2,"flow_LGV":np_sum2,"flow_OGV_1":np_sum2,"flow_OGV_2":np_sum2,"flow_HGV":np_sum2,"flow_BUS":np_sum2,"flow_bicycle":np_sum2,"flow_motorcycle": np_sum2, "flow_total":np_sum2, "average_speed_mph": "first"})

            #check for non-counts
            period_sum = 0
                
            delete_day = False
            
            #remove non-counts############
            for peak_hour in am_peak_datetime_objects:
                    period_sum += hourlydf2.loc[peak_hour, "flow_total"]
                
            if period_sum == 0:
                    delete_day = True
                    discard_record = hourlydf2[["site", "direction"]].head(1)
                    discarded_data = pd.concat([discarded_data, discard_record])
                    

            period_sum = 0
            for peak_hour in pm_peak_datetime_objects:
                    period_sum += hourlydf2.loc[peak_hour, "flow_total"]

                
            if period_sum == 0:
                    delete_day = True
                    discard_record = hourlydf2[["site", "direction"]].head(1)
                    discarded_data = pd.concat([discarded_data, discard_record])
            #################################

            #replace NaN values with blanks and delete corresponding rows for remaining tables
            nan_value = float("NaN")
            if delete_day == False:
                temp_df2.replace("", nan_value, inplace=True)
                temp_df2.dropna(subset = ["flow_total"], inplace=True)

                #recreate datetime column
                temp_df2 = temp_df2.reset_index()
                temp_df2 = temp_df2.rename(columns = {"index": "date"})

                #add data to output dataframe
                appended_data = pd.concat([appended_data, temp_df2])
            
            sheet_number +=1
    #define source field and output
    appended_data["Source"] = "Severnside ATC"
    appended_data.to_csv(f"{output_dir}\\ATC_2023_SEVERNSIDE.csv", index = False)


def format_ATC_2023_HCC_C_2(input_dir: str, output_dir: str, discarded_dir: str):
    
    #define required variables
    os.chdir(input_dir)
    files = glob.glob("*.csv")
    discarded_data = pd.DataFrame()
    #final database
    survey_database = pd.DataFrame()
    #time periods for non-count checks
    AM_PEAK_PERIOD = ["07:00", "8:00", "9:00"]
    PM_PEAK_PERIOD = ["16:00", "17:00", "18:00"]

    #loop through files
    for file in files:
        print("Adding ",file," to the database")
        file_path = os.path.join(input_dir,file)

        #count tables
        table = 0
        
        #load data in 117 rows at a time
        for data in pd.read_csv(file_path, chunksize = 117):

            #increment table number and read site number from top row
            table += 1
            site = str(data.columns.values[0])[-3:]
            print ("Table number : ", table, "   Site number :  ", site)
            
            #fix bug in site name
            if site == "001":
                    site = "611"
            else:
                    site = site[-3:]

            #read direction for each table
            dir = data.iloc[1,6][9:]
            #read date for each table
            date = str(data.iloc[1,3])
            
            #skip unneeded tables
            if dir == "Total Flow":
                continue
            
            #reset data range and columns to match table
            data = data.rename(columns = data.iloc[4])
            data = data[5:101]

            #convert data to numeric values to avoid string error
            data[['Car/lVan', 'Cr/lV+Tr', 'H. Van','LGV', 'Rigid', 'Rg+Tr', 'ArticHGV', 'Minibus', 'Bus' ]] = data[['Car/lVan', 'Cr/lV+Tr', 'H. Van','LGV', 'Rigid', 'Rg+Tr', 'ArticHGV', 'Minibus', 'Bus' ]].apply(pd.to_numeric)
            
            #define datetime, direction and site columns
            data['Date'] = date
            data['date'] = data['Date'] + " " + data['Begin']
            data['date'] = pd.to_datetime(data['date'])
            data['direction'] = dir
            data['site'] = site

            #prepare for non-count check
            am_peak_datetime_objects = []
            for peak_hour in AM_PEAK_PERIOD:
                    peak_hour = date + " " + peak_hour
                    peak_hour = datetime.datetime.strptime(peak_hour, '%d-%b-%y %H:%M')
                    am_peak_datetime_objects.append(peak_hour)
                    
            pm_peak_datetime_objects = []
            for peak_hour in PM_PEAK_PERIOD:
                    peak_hour = date + " " + peak_hour
                    peak_hour = datetime.datetime.strptime(peak_hour, '%d-%b-%y %H:%M')
                    pm_peak_datetime_objects.append(peak_hour)

            #aggregate flows into required user classes
            data['flow_car'] = data['Car/lVan'] + data['Cr/lV+Tr']
            data['flow_LGV'] = data['H. Van'] + data['LGV']
            data['flow_OGV_1'] = data['Rigid'] 
            data['flow_OGV_2'] = data['Rg+Tr'] + data['ArticHGV']
            data['flow_HGV'] = data['flow_OGV_1'] + data['flow_OGV_2']
            data['flow_BUS'] = data['Minibus'] + data['Bus']
            data['flow_total'] = data['flow_car'] + data['flow_LGV'] +  data['flow_HGV'] + data['flow_BUS']

            #drop unneeded columns
            data = data.drop(data.columns[[0,1,2,3,4,5,6,7,8,9,10,11,12,]],axis = 1)

            #set datetime index for resample
            data = data.set_index(["date"])

            #create resampled db and discarded non-count data
            np_sum = lambda x: x.values.sum()
            hourlydf = data.resample('60min').agg({"site":"first","direction":"first","flow_car":np_sum,"flow_LGV":np_sum,"flow_OGV_1":np_sum,"flow_OGV_2":np_sum,"flow_HGV":np_sum,"flow_BUS":np_sum,"flow_total":np_sum})


            period_sum = 0
            
            for peak_hour in am_peak_datetime_objects:
                    period_sum += hourlydf.loc[peak_hour, "flow_total"]
                
            if period_sum == 0:
                    discard_record = hourlydf[["site", "direction"]].head(1)
                    discarded_data = pd.concat([discarded_data, discard_record], ignore_index = True)
                    table += 1
                    continue

            period_sum = 0
            for peak_hour in pm_peak_datetime_objects:
                    period_sum += hourlydf.loc[peak_hour, "flow_total"]

                
            if period_sum == 0:
                    discard_record = hourlydf[["site", "direction"]].head(1)
                    discarded_data = pd.concat([discarded_data, discard_record], ignore_index = True)
                    table += 1
                    continue
            
            #delete non-count rows
            nan_value = float("NaN")
            data['Source'] = 'HCC ATC'
            data.replace("", nan_value, inplace=True)
            data.dropna(subset = ["flow_total"], inplace=True)

            #standardise direction naming
            if "East" in dir:
                data["direction"] = "East"

            elif "West" in dir:
                data["direction"] = "West"
                
            elif "North" in dir:
                data["direction"] = "North"
                
            elif "South" in dir:
                data["direction"] = "South"

            #reset and relabel datetime index
            data = data.reset_index()
            data = data.rename(columns = {"index": "date"})

            #append data to final database
            survey_database = pd.concat([survey_database,data], ignore_index = True)
    
    #define source column and output
    survey_database["Source"] = "HCC ATC"
    survey_database.to_csv(f"{output_dir}\\ATC_2023_HCC_C_2.csv", index = False)
    discarded_data.to_csv(f"{discarded_dir}\\Discarded_Data_ATC_2023_HCC_C_2.csv", index= False)



def Format_ATC_DBs():

    """
    Formats selected sets of data corresponding to the functions defined above. All arguments should match the directory defined in File_directories.py which 
    correspondings to the function and the year where necessary. The directory defintio  in File_directories.py should be checked to select the correct output
    directory argument. The folder structure of the raw data can also be used to check this.
    """


    #2019 NC
    #format_ATC_HCC_NC_1(File_directories.ATC_2019_NC_1, File_directories.ATC_2019_HOURLY_DB, File_directories.DISCARDED_DATA, "2019")
    #format_ATC_NC_2(File_directories.ATC_2019_NC_2, File_directories.ATC_2019_HOURLY_DB, "2019")
    #format_ATC_HCC_NC_3(File_directories.ATC_2019_NC_3, File_directories.ATC_2019_15M_DB, File_directories.DISCARDED_DATA, "2019")

    #2019 C
    #format_ATC_C_1_1(File_directories.ATC_2019_C_1_1, File_directories.ATC_2019_15M_DB, 2019, File_directories.DISCARDED_DATA)
    #format_ATC_HCC_C_1_2(File_directories.ATC_2019_C_1_2, File_directories.ATC_2019_15M_DB,File_directories.DISCARDED_DATA, 2019)

    #2022 NC
    #format_ATC_HCC_NC_1(File_directories.ATC_2022_NC_1,File_directories.ATC_2022_HOURLY_DB, File_directories.DISCARDED_DATA, "2022")
    #format_ATC_NC_2(File_directories.ATC_2022_NC_2,File_directories.ATC_2022_HOURLY_DB, "2022")
    #format_ATC_HCC_NC_3(File_directories.ATC_2022_NC_3,File_directories.ATC_2022_15M_DB, File_directories.DISCARDED_DATA, "2022")

    #2022 C
    #format_ATC_C_1_1(File_directories.ATC_2022_C_1_1, File_directories.ATC_2022_15M_DB, 2022, File_directories.DISCARDED_DATA)
    #ATC_HCC_C_1_2(File_directories.ATC_2022_C_1_2, File_directories.ATC_2022_15M_DB, File_directories.DISCARDED_DATA, 2022)
    #format_ATC_2022_C_2_1(File_directories.ATC_2022_C_2_1, File_directories.ATC_2022_15M_DB, File_directories.DISCARDED_DATA)
    #format_ATC_2022_C_2_2(File_directories.ATC_2022_C_2_2, File_directories.ATC_2022_15M_DB, File_directories.DISCARDED_DATA)

    #2023 NC
    #format_ATC_HCC_NC_1(File_directories.ATC_2023_HCC_NC_1, File_directories.ATC_2023_HOURLY_DB,File_directories.DISCARDED_DATA, "2023")
    #format_ATC_HCC_NC_3(File_directories.ATC_2023_NC_3,File_directories.ATC_2023_15M_DB, File_directories.DISCARDED_DATA, "2023")
    
    #2023_C
    format_ATC_C_1_1(File_directories.ATC_2023_HCC_C_1_1, File_directories.ATC_2023_15M_DB, 2023, File_directories.DISCARDED_DATA)
    #ATC_HCC_C_1_2(File_directories.ATC_2023_HCC_C_1_2, File_directories.ATC_2023_15M_DB, File_directories.DISCARDED_DATA, 2023)
    #format_ATC_2023_HCC_C_2(File_directories.ATC_2023_HCC_C_2, File_directories.ATC_2023_15M_DB,File_directories.DISCARDED_DATA)
    #format_ATC_2023_SEVERNSIDE(File_directories.ATC_2023_SEVERNSIDE_RAW,File_directories.ATC_2023_15M_DB)

    
if __name__ == "__main__":

    ########## ONLY UNCOMMENT IF RAW DATA HAS BEEN UPDATED OR WHEN FIRST RUNNING PROGRAM
    #copy_raw_data()
    ##########

    ########## ONLY UNCOMMENT WHEN FIRST RUNNING PROGRAM AFTER CLONING REPOSITORY. READ DOCSTRING OR README BEFORE RUNNING
    #copy_template_directory()
    ##########

    Format_ATC_DBs()