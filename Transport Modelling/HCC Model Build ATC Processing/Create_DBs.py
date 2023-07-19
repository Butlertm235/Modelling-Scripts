import pandas as pd
import os
import glob
import numpy as np
import datetime
import re



def format_ATC_2019_NC_1(input_dir: str, output_dir: str):
    os.chdir(input_dir)
    files = glob.glob("*.xlsx")
    survey_database = pd.DataFrame()
    for file in files:
        print("Adding ",file," to the database")
        sheets = pd.ExcelFile(file)
        sheet_names = sheets.sheet_names

        sitedf = pd.read_excel(file,skiprows=0,usecols="A",index_col=None,nrows=0)
        site = str(sitedf.columns.values[0])
        site = site[-3:]
        
        for table in range (2):
                skipr = 6+table*45

                dirdf = pd.read_excel(file,skiprows = skipr - 4,usecols="G",index_col=None,nrows=0)
                if len(dirdf. columns) == 0:
                    break
                
                dir = dirdf.columns.values[0][9:]
            
                if dir == "Total Flow":
                    continue
                if dir == "Errors":
                    continue
        
                
                df = pd.read_excel(file,skiprows = skipr -1,nrows=24,dtype={0:str})
                tabledf = pd.read_excel(file,skiprows=6,usecols="A:H",index_col=None,nrows=24)
                formatdf = pd.DataFrame(columns = ['date', 'flow_total'])

                for day in range(7):
                    survey_date = str(df.columns.values[day+1].date())

                    day_df = pd.DataFrame(columns = ['date','flow_total'])
                    day_df['date'] = survey_date + " " + df['Begin']
                    day_df['date']= pd.to_datetime(day_df['date'])
                    day_df['flow_total'] = df[df.columns.values[day+1]]
                    
                    nan_value = float("NaN")


                    day_df.replace("", nan_value, inplace=True)

                    day_df.dropna(subset = ["flow_total"], inplace=True)

                    
                    
                    formatdf = formatdf.append(day_df)
                    
                
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

                formatdf['site'] = site
                #formatdf['direction'] = dir
                formatdf['Source'] = 'ATC'
                formatdf['flow_car'] = ''
                formatdf['flow_LGV'] = ''
                formatdf['flow_OGV_1'] = ''
                formatdf['flow_OGV_2'] = ''
                formatdf['flow_HGV'] = ''

                survey_database = survey_database.append(formatdf)
    survey_database["Source"] = "HCC ATC"
    survey_database.to_csv(f"{output_dir}\\ATC_2019_NC_1.csv", index = False)

def format_ATC_2019_NC_2(input_dir: str, output_dir: str):
    os.chdir(input_dir)
    files = glob.glob("*.xlsx")
    survey_database = pd.DataFrame()
    for file in files:
        print("Adding ",file," to the database")
        sheets = pd.ExcelFile(file)
        sheet_names = sheets.sheet_names
        a = len(sheet_names)
        
        for date in sheet_names[0:a]:
        
            sitedf = pd.read_excel(file,sheet_name=date,skiprows=0,usecols="B",index_col=None,nrows=0)
            site = str(sitedf.columns.values[0])
            site = site[-3:] 
        
            for table in range (2):
                skipr = 5+table*46
            
                dirdf = pd.read_excel(file,sheet_name=date,skiprows = skipr - 3,usecols="I",index_col=None,nrows=0)
                if len(dirdf. columns) == 0:
                    break
                
                dir = dirdf.columns.values[0]
            
                if dir == "Total Flow":
                    continue

                df = pd.read_excel(file,sheet_name=date,skiprows = skipr ,nrows=24,dtype={0:str})
                

                formatdf = pd.DataFrame(columns = ['date', 'flow_total', "direction"])
                
                for day in range(7):
                    survey_date = str(df.columns.values[day+1].date())

                    day_df = pd.DataFrame(columns = ['date','flow_total'])
                    day_df['date'] = survey_date + " " + df['Unnamed: 0']
                    day_df['date']= pd.to_datetime(day_df['date'])
                    day_df['flow_total'] = df[df.columns.values[day+1]]
                    nan_value = float("NaN")

                    day_df.replace("", nan_value, inplace=True)

                    day_df.dropna(subset = ["flow_total"], inplace=True)
                
                    formatdf = formatdf.append(day_df)

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

                formatdf['site'] = site
                #formatdf['direction'] = dir
                formatdf['Source'] = 'ATC'
                formatdf['flow_car'] = ''
                formatdf['flow_LGV'] = ''
                formatdf['flow_OGV_1'] = ''
                formatdf['flow_OGV_2'] = ''
                formatdf['flow_HGV'] = ''

                survey_database = survey_database.append(formatdf)
    survey_database["Source"] = "HCC ATC"
    survey_database.to_csv(f"{output_dir}\\ATC_2019_NC_2.csv", index = False)


def format_ATC_2019_NC_3(input_dir: str, output_dir: str):
    
    os.chdir(input_dir)
    files = glob.glob("*.xlsx")
    survey_database = pd.DataFrame()
    Time_CSV = pd.read_csv("Time.csv")
    Time_CSV['Begin'] = pd.to_datetime(Time_CSV['Begin']).dt.time
    for file in files:
        print("Adding ",file," to the database")
        sheets = pd.ExcelFile(file)
        sheet_names = sheets.sheet_names
        
        sitedf = pd.read_excel(file,skiprows=0,usecols="A",index_col=None,nrows=0)
        site = str(sitedf.columns.values[0])
        site = site[-3:]
        
        
        for table in range (6):
            skipr = 5+table*117
            
            dirdf = pd.read_excel(file,skiprows = skipr - 3,usecols="G",index_col=None,nrows=0)
            if len(dirdf. columns) == 0:
                break
                
            dir = dirdf.columns.values[0][9:]
            
            if dir == "Total Flow":
                    continue

            df = pd.read_excel(file,skiprows = skipr ,nrows=96,dtype={0:str})
            
            
            formatdf = pd.DataFrame(columns = ['date','flow_total'])

            for day in range(7):
                survey_date = str(df.columns.values[day+1].date())

                print(survey_date)
                day_df = pd.DataFrame(columns = ['time','date','Total Flow','flow_total'])
                day_df['date'] = survey_date + " " + df['Begin']
                day_df['date']= pd.to_datetime(day_df['date'])
                #day_df['time']=day_df['date'].dt.time
                print(df.columns.values[day+1])
                day_df['flow_total'] = df[df.columns.values[day+1]]

                print("check 2:")
                #print(day_df)
                #print(df[df.columns.values[day+1]])
                #day_df['flow_total'] = day_df['Total Flow'].rolling(4).sum().shift(periods = -3)
                print(day_df)
                print(df[df.columns.values[day+1]])
                nan_value = float("NaN")
                #day_df = pd.merge(Time_CSV,day_df,how="inner", left_on=['Begin'],right_on=['time']) 
                print(day_df)
                day_df.replace("", nan_value, inplace=True)

                day_df.dropna(subset = ["flow_total"], inplace=True)
                #print(day_df)

                formatdf = formatdf.append(day_df)
                
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

                formatdf['site'] = site
                #formatdf['direction'] = dir
                formatdf['Source'] = 'ATC'
                formatdf['flow_car'] = ''
                formatdf['flow_LGV'] = ''
                formatdf['flow_OGV_1'] = ''
                formatdf['flow_OGV_2'] = ''
                formatdf['flow_HGV'] = ''
                print("Check 3:")
                print(formatdf)

            survey_database = survey_database.append(formatdf)
            
    survey_database = survey_database.drop(columns = ['time', 'Total Flow'])
    survey_database["Source"] = "HCC ATC"
    survey_database.to_csv(f"{output_dir}\\ATC_2019_NC_3.csv", index = False)

def format_ATC_C_1_1(input_dir: str, output_dir: str, year: int, discarded_dir: str):
    
    os.chdir(input_dir)
    files = glob.glob("*.csv")
    survey_database = pd.DataFrame()
    discarded_data = pd.DataFrame()

    AM_PEAK_PERIOD = ["07:00", "08:00", "09:00"]
    PM_PEAK_PERIOD = ["16:00", "17:00", "18:00"]

    for file in files:
        print("Adding ",file," to the database")
        
        table = 0
        i = 1
        n = -1
        
        for data in pd.read_csv(file, chunksize = 117):
            table = table + i
            site = str(data.columns.values[0])[-3:]
            #print ("Table number : ", table, "   Site number :  ", site)
            #print (sheet_names)

            dir = data.iloc[1,6][9:]
            
            date = str(data.iloc[1,3])
            
            #if len(date) == 0:
                #break
            if dir == "Total Flow":
                continue
            #if dir == "Northbound" or dir == "Southbound" or dir == "Eastbound" or dir =="Westbound" or dir =="Westbound 1" or dir =="Westbound 2" or dir =="Westbound 3" or dir =="Westbound 1" or dir =="Eastbound 1" or dir =="Eastbound 2" or dir =="Eastbound OS" or dir =="Eastbound NS":
                #continue
            #if dir == "Westbound OS" or dir == "Westbound NS" or dir == "Northbound 1" or dir == "Northbound 2" or dir == "Southbound 1" or dir == "Southbound 2" or dir == "Southbound 3":
                #continue
            #if dir == "Southwest 1" or dir == "Southwest 2" or dir == "Southeast 1" or dir == "Southeast 2" or dir == "Northwest 1" or dir == "Northwest 2" or dir == "Northeast 1" or dir == "Northeast 2" or dir == "Total Flow":
                #continue
            
            data = data.rename(columns = data.iloc[4])
            data = data[5:101]
            data[['Car/lVan', 'Cr/lV+Tr', 'H. Van','LGV', 'Rigid', 'Rg+Tr', 'ArticHGV', 'Minibus', 'Bus' ]] = data[['Car/lVan', 'Cr/lV+Tr', 'H. Van','LGV', 'Rigid', 'Rg+Tr', 'ArticHGV', 'Minibus', 'Bus' ]].apply(pd.to_numeric)
            data['Date'] = date

            data['date'] = data['Date'] + " " + data['Begin']
        
            data['date'] = pd.to_datetime(data['date'])
            
            #Converts directions to uniform style
            #if (("South" in dir) and (("West" in dir) or ("East" in dir))) or (("North" in dir) and (("West" in dir) or ("East" in dir))):
                #print("Multi direction")
            if "East" in dir:
                data["direction"] = "East"

            elif "West" in dir:
                data["direction"] = "West"
            
            elif "North" in dir:
                data["direction"] = "North"
            
            elif "South" in dir:
                data["direction"] = "South"

            data['site'] = site

                #rename first column to Total. Keep up to col N, and column U

            data['flow_car'] = data['Car/lVan'] + data['Cr/lV+Tr']
            data['flow_LGV'] = data['H. Van'] + data['LGV']
            data['flow_OGV_1'] = data['Rigid'] 
            data['flow_OGV_2'] = data['Rg+Tr'] + data['ArticHGV']
            data['flow_HGV'] = data['flow_OGV_1'] + data['flow_OGV_2']
            data['flow_BUS'] = data['Minibus'] + data['Bus']
            data['flow_total'] = data['flow_car'] + data['flow_LGV'] +  data['flow_HGV'] + data['flow_BUS']
            data[["flow_car", "flow_LGV", "flow_OGV_1", "flow_OGV_2", "flow_HGV", "flow_BUS", "flow_total"]] = data[["flow_car", "flow_LGV", "flow_OGV_1", "flow_OGV_2", "flow_HGV", "flow_BUS", "flow_total"]].fillna(0)
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
            
            data = data.set_index('date')
            np_sum = lambda x: x.values.sum()
            hourly_data_table = data.resample('60min').agg({"site":"first","direction":"first","flow_car":np_sum,"flow_LGV":np_sum,"flow_OGV_1":np_sum,"flow_OGV_2":np_sum,"flow_HGV":np_sum,"flow_BUS":np_sum,"flow_total":np_sum})
            hourly_data_table["flow_total"] = hourly_data_table["flow_total"].fillna(0)
            period_sum = 0
            
            for peak_hour in am_peak_datetime_objects:
                period_sum += hourly_data_table.loc[peak_hour, "flow_total"]
            
            if period_sum == 0:
                discard_record = hourly_data_table[["site", "direction"]].head(1)
                discarded_data = pd.concat([discarded_data, discard_record])
                continue

            period_sum = 0
            for peak_hour in pm_peak_datetime_objects:
                period_sum += hourly_data_table.loc[peak_hour, "flow_total"]

            if period_sum == 0:
                discard_record = hourly_data_table[["site", "direction"]].head(1)
                discarded_data = pd.concat([discarded_data, discard_record])
                continue

            data = data.reset_index()
            data = data.rename(columns = {"index": "date"})

            data = data.drop(data.columns[[1,2,3,4,5,6,7,8,9,10,11,12,13]],axis = 1)

            survey_database = survey_database.append(data)
    survey_database["Source"] = "HCC ATC"
    survey_database.to_csv(f"{output_dir}\\ATC_{str(year)}_C_1_1.csv", index = False)
    discarded_data.to_csv(f"{discarded_dir}\\ATC_{str(year)}_C_1_1_Discarded_Data.csv", index=False)


def format_ATC_C_1_2(input_dir: str, output_dir: str, year: int):

    os.chdir(input_dir)
    files = glob.glob("*.csv")
    survey_database = pd.DataFrame()
    
    for file in files:
        print("Adding ",file," to the database")
        
        table = 0
        i = 1
        n = -1
        
        for data in pd.read_csv(file, chunksize = 117):
            table = table + i
            site = str(data.columns.values[0])[-3:]

            dir = data.iloc[1,6][9:]
            
            date = str(data.iloc[1,3])
            
            if dir == "North" or dir == "South" or dir == "East" or dir =="West" or dir == "Northbound " or dir == "Southbound " or dir == "Eastbound " or dir == "Westbound ":
                data = data.rename(columns = data.iloc[4])
                data = data[5:101]
                data[['Car/lVan', 'Cr/lV+Tr', 'H. Van','LGV', 'Rigid', 'Rg+Tr', 'ArticHGV', 'Minibus', 'Bus' ]] = data[['Car/lVan', 'Cr/lV+Tr', 'H. Van','LGV', 'Rigid', 'Rg+Tr', 'ArticHGV', 'Minibus', 'Bus' ]].apply(pd.to_numeric)
                data['Date'] = date

                data['date'] = data['Date'] + " " + data['Begin']
            
                data['date'] = pd.to_datetime(data['date'])
                
                data['direction'] = dir
                data['site'] = site

                    #rename first column to Total. Keep up to col N, and column U
                        
                    #df = df.rename(columns={'Unnamed: 0':'tt'})

                data['flow_car'] = data['Car/lVan'] + data['Cr/lV+Tr']
                data['flow_LGV'] = data['H. Van'] + data['LGV']
                data['flow_OGV_1'] = data['Rigid'] 
                data['flow_OGV_2'] = data['Rg+Tr'] + data['ArticHGV']
                data['flow_HGV'] = data['flow_OGV_1'] + data['flow_OGV_2']
                data['flow_BUS'] = data['Minibus'] + data['Bus']
                data['flow_total'] = data['flow_car'] + data['flow_LGV'] +  data['flow_HGV'] + data['flow_BUS']

                data = data.drop(data.columns[[0,1,2,3,4,5,6,7,8,9,10,11,12,]],axis = 1)

                survey_database = survey_database.append(data)
            #print(survey_database)

    survey_database = survey_database.drop(columns = ["Date"])
    survey_database["Source"] = "HCC ATC"
    survey_database.to_csv(f"{output_dir}\\ATC_{str(year)}_C_1_2.csv", index = False)


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
                
                data_table['direction'] = direction
                data_table['site'] = site

                data_table['Date'] = date
                data_table["time"] = data_table['time'].astype(str)
                data_table["date"] = data_table["Date"] + " " + data_table["time"]
                data_table['date'] = pd.to_datetime(data_table['date'])

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
                if "East" in direction:
                    data_table["direction"] = "East"

                elif "West" in direction:
                    data_table["direction"] = "West"
                
                elif "North" in direction:
                    data_table["direction"] = "North"
                
                elif "South" in direction:
                    data_table["direction"] = "South"
                
                #Adds new data to the end of compiled data

                data_table = data_table.reset_index()
                data_table = data_table.rename(columns = {"index": "date"})

                appended_data = pd.concat([appended_data, data_table])
                table_number += 1
            sheet_number += 1

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
                data_table['date'] = pd.to_datetime(data_table['date'])

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






#Output to hourly
def format_ATC_2022_NC_1(input_dir: str, output_dir: str):
    os.chdir(input_dir)
    files = glob.glob("*.xlsx")
    survey_database = pd.DataFrame()

    for file in files:
        print("Adding ",file," to the database")
        sheets = pd.ExcelFile(file)
        sheet_names = sheets.sheet_names


        sitedf = pd.read_excel(file,skiprows=0,usecols="A",index_col=None,nrows=0)
        site = str(sitedf.columns.values[0])
        site = site[-3:]
        for table in range (2):

            skipr = 6+table*45
           
            dirdf = pd.read_excel(file,skiprows = skipr - 4,usecols="G",index_col=None,nrows=0)
            if len(dirdf. columns) == 0:
                break
            
            dir = dirdf.columns.values[0][9:]
        
            if dir == "Total Flow":
                continue
            if dir == "Errors":
                continue
            df = pd.read_excel(file,skiprows = skipr -1,nrows=24,dtype={0:str})
            tabledf = pd.read_excel(file,skiprows=6,usecols="A:H",index_col=None,nrows=24)
            formatdf = pd.DataFrame(columns = ['date', 'flow_total'])

            for day in range(7):
                survey_date = str(df.columns.values[day+1].date())

                day_df = pd.DataFrame(columns = ['date','flow_total'])
                day_df['date'] = survey_date + " " + df['Begin']
                day_df['date']= pd.to_datetime(day_df['date'])
                day_df['flow_total'] = df[df.columns.values[day+1]]
                
                nan_value = float("NaN")


                day_df.replace("", nan_value, inplace=True)

                day_df.dropna(subset = ["flow_total"], inplace=True)

                formatdf = formatdf.append(day_df)
                
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

            formatdf['site'] = site
            #formatdf['direction'] = dir
            formatdf['Source'] = 'ATC'
            formatdf['flow_car'] = ''
            formatdf['flow_LGV'] = ''
            formatdf['flow_OGV_1'] = ''
            formatdf['flow_OGV_2'] = ''
            formatdf['flow_HGV'] = ''

            survey_database = survey_database.append(formatdf)
    survey_database["Source"] = "HCC ATC"
    survey_database.to_csv(f"{output_dir}\\ATC_2022_NC_1.csv", index = False)


#Output to hourly
def format_ATC_2022_NC_2(input_dir: str, output_dir: str):
    os.chdir(input_dir)
    
    files = glob.glob("*.xlsx")
    survey_database = pd.DataFrame()

    for file in files:
        print("Adding ",file," to the database")
        sheets = pd.ExcelFile(file)
        sheet_names = sheets.sheet_names

        sitedf = pd.read_excel(file,skiprows=0,usecols="A",index_col=None,nrows=0)
        site = str(sitedf.columns.values[0])
        site = site[-3:]

        for table in range (6):
            skipr = 6+table*45
           
            dirdf = pd.read_excel(file,skiprows = skipr - 4,usecols="G",index_col=None,nrows=0)
            if len(dirdf. columns) == 0:
                break
            
            dir = dirdf.columns.values[0][9:]

        
            if dir == "Total Flow":
                continue
    
            df = pd.read_excel(file,skiprows = skipr -1,nrows=24,dtype={0:str})
            tabledf = pd.read_excel(file,skiprows=6,usecols="A:H",index_col=None,nrows=24)
            formatdf = pd.DataFrame(columns = ['date', 'flow_total'])

            for day in range(7):
                survey_date = str(df.columns.values[day+1].date())

                day_df = pd.DataFrame(columns = ['date','flow_total'])
                day_df['date'] = survey_date + " " + df['Begin']
                day_df['date']= pd.to_datetime(day_df['date'])
                day_df['flow_total'] = df[df.columns.values[day+1]]
                
                nan_value = float("NaN")


                day_df.replace("", nan_value, inplace=True)

                day_df.dropna(subset = ["flow_total"], inplace=True)

                
                
                formatdf = formatdf.append(day_df)
                
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

            formatdf['site'] = site
            #formatdf['direction'] = dir
            formatdf['Source'] = 'ATC'
            formatdf['flow_car'] = ''
            formatdf['flow_LGV'] = ''
            formatdf['flow_OGV_1'] = ''
            formatdf['flow_OGV_2'] = ''
            formatdf['flow_HGV'] = ''


            
            
            
            survey_database = survey_database.append(formatdf)
    survey_database["Source"] = "HCC ATC"
    survey_database.to_csv(f"{output_dir}\\ATC_2022_NC_2.csv", index = False)


def format_ATC_2022_NC_3(input_dir: str, output_dir: str):

    os.chdir(input_dir)
    files = glob.glob("*.xlsx")
    survey_database = pd.DataFrame()

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
        
            #Loop through directions
            for table in range (3):
                skipr = 5+table*46


                #Record direction
                dirdf = pd.read_excel(file,sheet_name=date,skiprows = skipr - 3,usecols="I",index_col=None,nrows=0)
                if len(dirdf. columns) == 0:
                    break
                dir = dirdf.columns.values[0]

                if dir == "":
                    continue
                #Skip 2-way flow
                if dir == "Total Flow":
                    continue
                if dir == "Errors":
                    continue
                if site == "536":
                    if "bound" in dir:
                        continue
                #Read raw data tab;e
                df = pd.read_excel(file,sheet_name=date,skiprows = skipr ,nrows=24,dtype={0:str})
                #tabledf = pd.read_excel(file,sheet_name=date,skiprows=6,usecols="A:H",index_col=None,nrows=24)


                #Create dataframe per table to store all values in two columns
                formatdf = pd.DataFrame(columns = ['date', 'flow_total'])
                
                #Loop around days
                for day in range(7):
                    #Record date from column header
                    survey_date = str(df.columns.values[day+1].date())

                    #df for specific column in each table
                    day_df = pd.DataFrame(columns = ['date','flow_total'])

                    #create datetime
                    day_df['date'] = survey_date + " " + df['Unnamed: 0']
                    day_df['date']= pd.to_datetime(day_df['date'])

                    #Record flow value
                    day_df['flow_total'] = df[df.columns.values[day+1]]

                    #Replace NA
                    nan_value = float("NaN")
                    day_df.replace("", nan_value, inplace=True)
                    day_df.dropna(subset = ["flow_total"], inplace=True)

                    #Append to main df for that table
                    formatdf = formatdf.append(day_df)
                
                         


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
                
                formatdf['site'] = site
                #formatdf['direction'] = dir
                formatdf['Source'] = 'ATC'
                formatdf['flow_car'] = ''
                formatdf['flow_LGV'] = ''
                formatdf['flow_OGV_1'] = ''
                formatdf['flow_OGV_2'] = ''
                formatdf['flow_HGV'] = ''

                survey_database = survey_database.append(formatdf)
    survey_database["Source"] = "HCC ATC"
    survey_database.to_csv(f"{output_dir}\\ATC_2022_NC_3.csv", index = False)




def format_ATC_2022_NC_4(input_dir: str, output_dir: str):
    os.chdir(input_dir)
    files = glob.glob("*.xlsx")
    survey_database = pd.DataFrame()

    for file in files:
        print("Adding ",file," to the database")
        sheets = pd.ExcelFile(file)
        sheet_names = sheets.sheet_names
        
        sitedf = pd.read_excel(file,skiprows=0,usecols="A",index_col=None,nrows=0)
        site = str(sitedf.columns.values[0])
        site = site[-3:]
        
        sitedf = pd.read_excel(file,skiprows=0,usecols="A",index_col=None,nrows=0)
        site = str(sitedf.columns.values[0])
        site = site[-3:]
        
        for table in range (6):
            skipr = 5+table*117


            
            dirdf = pd.read_excel(file,skiprows = skipr - 3,usecols="G",index_col=None,nrows=0)
            if len(dirdf. columns) == 0:
                break
                
            
                
            dir = dirdf.columns.values[0][9:]
            
            if dir == "Total Flow":
                continue
            
            



            df = pd.read_excel(file,skiprows = skipr ,nrows=96,dtype={0:str})
            
            
            formatdf = pd.DataFrame(columns = ['date','flow_total'])

            for day in range(7):
                survey_date = str(df.columns.values[day+1].date())
                day_df = pd.DataFrame(columns = ['time','date','flow_total'])
                day_df['date'] = survey_date + " " + df['Begin']
                day_df['date']= pd.to_datetime(day_df['date'])
                day_df['flow_total'] = df[df.columns.values[day+1]]
                
                nan_value = float("NaN")

                day_df.replace("", nan_value, inplace=True)

                day_df.dropna(subset = ["flow_total"], inplace=True)

                formatdf = formatdf.append(day_df)
                
                formatdf['site'] = site

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

                formatdf['Source'] = 'ATC'
                formatdf['flow_car'] = ''
                formatdf['flow_LGV'] = ''
                formatdf['flow_OGV_1'] = ''
                formatdf['flow_OGV_2'] = ''
                formatdf['flow_HGV'] = ''

            survey_database = survey_database.append(formatdf)

    survey_database = survey_database.drop(columns = ["time"])
    survey_database["Source"] = "HCC ATC"
    survey_database.to_csv(f"{output_dir}\\ATC_2022_NC_4.csv", index = False)

def format_ATC_2023_SEVERNSIDE(input_dir: str, output_dir: str):

    os.chdir(input_dir)

    time_filter = pd.read_csv("Time_Filter.csv")
    time_filter['TIME'] = time_filter['TIME'].astype(str)

    
    file_names = glob.glob("*.xlsm")

    AM_PEAK_PERIOD = ["07:00", "08:00", "09:00"]
    PM_PEAK_PERIOD = ["16:00", "17:00", "18:00"]

    appended_data = pd.DataFrame(columns = ["date","site", "direction", 'flow_car', 'flow_LGV', 'flow_OGV_1', "flow_OGV_2", "flow_HGV",'flow_BUS', 'flow_total'])
    discarded_data = pd.DataFrame()

    print(file_names)

    for file in file_names:
        print("Adding ", file, " to the database")

        site =  re.findall('\s[0-9]+', file)[0][1:]
        print(site)
        sheets = pd.ExcelFile(file)
        sheet_names = sheets.sheet_names
        sheets.close()

        sheet_number = 1


        for name in sheet_names:

            print(f"Processing sheet {name}")

            if(name[2] != " "):
                continue
            if(name[3] == "m"):
                continue
            
            date = name
            date = date.replace(" ",r"/")
            print(date)
            temp_df = pd.DataFrame(columns = ["date","site", "direction", 'flow_car', 'flow_LGV', 'flow_OGV_1', "flow_OGV_2",'flow_BUS', "flow_HGV", "flow_bicycle", "flow_motorcycle", 'flow_total', "average_speed_mph"])
            temp_df2 = pd.DataFrame(columns = ["date","site", "direction", 'flow_car', 'flow_LGV', 'flow_OGV_1', "flow_OGV_2",'flow_BUS', "flow_HGV", "flow_bicycle", "flow_motorcycle", 'flow_total', "average_speed_mph"])

            data = pd.read_excel(file, sheet_name = name)


            dir1 = data.iloc[6,1][14:]
            dir2 = data.iloc[6,13][14:]
            data.columns = data.iloc[7]
            data = data[8:294]

            data["TIME"] = data["TIME"].astype(str)

            data = pd.merge(data,time_filter, how = "inner", on = "TIME")

            temp_df['date'] = date + " " + data["TIME"]
            temp_df['date'] = pd.to_datetime(temp_df['date'], dayfirst = True)
            temp_df["site"] = site
            temp_df["direction"] = dir1
            temp_df.iloc[:,[3,4,5,6,7,9,10,12]] = data.iloc[:,[1,2,3,4,5,7,6,8]]
            temp_df["flow_HGV"] = temp_df['flow_OGV_1'] + temp_df['flow_OGV_2']
            temp_df["flow_total"] = temp_df['flow_car'] + temp_df['flow_LGV'] + temp_df['flow_HGV'] + temp_df2['flow_BUS'] + temp_df["flow_bicycle"] + temp_df["flow_motorcycle"]
            
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

            temp_df = temp_df.set_index(["date"])

            np_sum = lambda x: x.values.sum()
            np_avg = lambda x: x.values.mean()
            hourlydf = temp_df.resample('60min').agg({"site":"first","direction":"first","flow_car":np_sum,"flow_LGV":np_sum,"flow_OGV_1":np_sum,"flow_OGV_2":np_sum,"flow_HGV":np_sum,"flow_BUS":np_sum,"flow_bicycle":np_sum,"flow_motorcycle": np_sum, "flow_total":np_sum, "average_speed_mph": "first"})

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
                    
            
            nan_value = float("NaN")
            if delete_day == False:
                temp_df.replace("", nan_value, inplace=True)

                temp_df.dropna(subset = ["flow_total"], inplace=True)

                temp_df = temp_df.reset_index()
                temp_df = temp_df.rename(columns = {"index": "date"})

                appended_data = pd.concat([appended_data, temp_df])
            

            temp_df2['date'] = date + " " + data["TIME"]
            temp_df2['date'] = pd.to_datetime(temp_df2['date'], dayfirst = True)
            temp_df2["site"] = site
            temp_df2["direction"] = dir2
            temp_df2.iloc[:,[3,4,5,6,7,9,10,12]] = data.iloc[:,[13,14,15,16,17,18,19,20]]
            temp_df2["flow_HGV"] = temp_df2['flow_OGV_1'] + temp_df2['flow_OGV_2']
            temp_df2["flow_total"] = temp_df2['flow_car'] + temp_df2['flow_LGV'] + temp_df2['flow_HGV'] + temp_df2['flow_BUS'] + temp_df2["flow_bicycle"] + temp_df2["flow_motorcycle"]


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


            temp_df2 = temp_df2.set_index(["date"])

            np_sum2 = lambda x: x.values.sum()
            np_avg2 = lambda x: x.values.mean()
            hourlydf2 = temp_df2.resample('60min').agg({"site":"first","direction":"first","flow_car":np_sum2,"flow_LGV":np_sum2,"flow_OGV_1":np_sum2,"flow_OGV_2":np_sum2,"flow_HGV":np_sum2,"flow_BUS":np_sum2,"flow_bicycle":np_sum2,"flow_motorcycle": np_sum2, "flow_total":np_sum2, "average_speed_mph": "first"})

            period_sum = 0
                
            delete_day = False
            
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
                    

            nan_value = float("NaN")
            if delete_day == False:
                temp_df2.replace("", nan_value, inplace=True)

                temp_df2.dropna(subset = ["flow_total"], inplace=True)

                temp_df2 = temp_df2.reset_index()
                temp_df2 = temp_df2.rename(columns = {"index": "date"})

                appended_data = pd.concat([appended_data, temp_df2])
            
            sheet_number +=1

    appended_data["Source"] = "Severnside ATC"
    appended_data.to_csv(f"{output_dir}\\ATC_2023_SEVERNSIDE.csv", index = False)

def format_ATC_2023_HCC_NC_1(input_dir: str, output_dir: str, discarded_dir: str):
    os.chdir(input_dir)

    discarded_data = pd.DataFrame()

    files = glob.glob("*.xlsx")
    survey_database = pd.DataFrame()
    AM_PEAK_PERIOD = ["07:00", "8:00", "9:00"]
    PM_PEAK_PERIOD = ["16:00", "17:00", "18:00"]

    for file in files:
        print("Adding ",file," to the database")
        file_path = os.path.join(input_dir,file)
        
        sitedf = pd.read_excel(file_path,skiprows=0,usecols="A",index_col=None,nrows=0)
        site = str(sitedf.columns.values[0])
        site = site[-3:]
        
        for table in range (100):
                skipr = 6+table*45


                dirdf = pd.read_excel(file_path,skiprows = skipr - 4,usecols="G",index_col=None,nrows=0)
                if len(dirdf. columns) == 0:
                    break
                
                dir = dirdf.columns.values[0]
            
                if dir == "Channel: Total Flow":
                    continue

                if dir == "Channel: Errors":
                    continue

                if "Cycles" in dir:
                    continue
                
                if "Traffic" in dir:
                    continue

                df = pd.read_excel(file_path,skiprows = skipr -1,nrows=24,dtype={0:str})
                tabledf = pd.read_excel(file_path,skiprows=6,usecols="A:H",index_col=None,nrows=24)
                formatdf = pd.DataFrame(columns = ['flow_total'])

                for day in range(7):
                    survey_date = str(df.columns.values[day+1].date())

                    day_df = pd.DataFrame(columns = ['date','flow_total'])
                    day_df['date'] = survey_date + " " + df['Begin']
                    day_df['date']= pd.to_datetime(day_df['date'])
                    day_df['flow_total'] = df[df.columns.values[day+1]]

                    day_df['site'] = site
                    day_df['direction'] = dir

                    day_df = day_df.set_index(["date"])

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

                
                    period_sum = 0

                    for peak_hour in am_peak_datetime_objects:
                        period_sum += day_df.loc[peak_hour, "flow_total"]
        
                
                    if period_sum == 0:
                        discard_record = day_df[["site", "direction"]].head(1)
                        discarded_data = pd.concat([discarded_data, discard_record], ignore_index = True)
                        continue

                    period_sum = 0
                    for peak_hour in pm_peak_datetime_objects:
                        period_sum += day_df.loc[peak_hour, "flow_total"]
                        print(f"Time period {peak_hour} sum is: {period_sum}")
                    #print(peak_hour)

                    if period_sum == 0:
                        discard_record = day_df[["site", "direction"]].head(1)
                        discarded_data = pd.concat([discarded_data, discard_record])
                        continue
                        
                    formatdf = pd.concat([formatdf,day_df])

                    formatdf['Source'] = 'ATC'
                    formatdf['flow_car'] = ''
                    formatdf['flow_LGV'] = ''
                    formatdf['flow_OGV1'] = ''
                    formatdf['flow_OGV2'] = ''
                    formatdf['flow_HGV'] = ''
                
                nan_value = float("NaN")

                formatdf.replace("", nan_value, inplace=True)

                formatdf.dropna(subset = ["flow_total"], inplace=True)

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

                formatdf = formatdf.reset_index()
                formatdf = formatdf.rename(columns={"index":"date"})

                survey_database = pd.concat([survey_database,formatdf], ignore_index = True)
    
    survey_database["Source"] = "HCC ATC"
    survey_database.to_csv(f"{output_dir}\\ATC_2023_HCC_NC_HOURLY_DBs.csv", index = False)
    discarded_data.to_csv(f"{discarded_dir}\\Discarded_Data_ATC_2023_HCC_NC_HOURLY_DBs.csv", index = False)

def format_ATC_2023_HCC_NC_2(input_dir: str, output_dir: str, discarded_dir: str):

    os.chdir(input_dir)

    AM_PEAK_PERIOD = ["07:00", "8:00", "9:00"]
    PM_PEAK_PERIOD = ["16:00", "17:00", "18:00"]
    files = glob.glob("*.xlsx")
    survey_database = pd.DataFrame()
    discarded_data = pd.DataFrame() 

    for file in files:
        print("Adding ",file," to the database")
        file_path = os.path.join(input_dir,file)
        
        sitedf = pd.read_excel(file_path,skiprows=0,usecols="A",index_col=None,nrows=0)
        site = str(sitedf.columns.values[0])
        site = site[-3:]
        
        for table in range (100):
            skipr = 5+table*117

            dirdf = pd.read_excel(file_path,skiprows = skipr - 3,usecols="G",index_col=None,nrows=0)
            if len(dirdf. columns) == 0:
                break
                  
            dir = dirdf.columns.values[0]
            
            if dir == "Channel: Total Flow":
                    continue

            df = pd.read_excel(file_path,skiprows = skipr ,nrows=96,dtype={0:str})
            formatdf = pd.DataFrame(columns = ['flow_total'])

            for day in range(7):
                    
                    survey_date = str(df.columns.values[day+1].date())
                    day_df = pd.DataFrame(columns = ['date','flow_total'])
                    day_df['date'] = survey_date + " " + df['Begin']
                    day_df['date']= pd.to_datetime(day_df['date'])
                    day_df['flow_total'] = df[df.columns.values[day+1]]

                    day_df['site'] = site
                    day_df['direction'] = dir


                    day_df = day_df.set_index("date")

                    #day_df = day_df.set_index(["date"])

                    #hourlydf = pd.concat([day_df,hourlydf])

                    #np_sum = lambda x: x.values.sum()
                    #hourlydf = hourlydf.resample('60min').agg({"site":"first","direction":"first","flow_total":np_sum})

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


                    #hourlydf = pd.concat([day_df,hourlydf])

                    np_sum = lambda x: x.values.sum()
                    hourly_data_table = day_df.resample('60min').agg({"site":"first","direction":"first","flow_total":np_sum})

                
                    period_sum = 0

                    for peak_hour in am_peak_datetime_objects:
                        period_sum += hourly_data_table.loc[peak_hour, "flow_total"]
                    
                    if period_sum == 0:
                        discard_record = hourly_data_table[["site", "direction"]].head(1)
                        discarded_data = pd.concat([discarded_data, discard_record], ignore_index = True)
                        #table_number += 1
                        continue

                    period_sum = 0
                    for peak_hour in pm_peak_datetime_objects:
                        period_sum += hourly_data_table.loc[peak_hour, "flow_total"]

                    if period_sum == 0:
                        discard_record = hourly_data_table[["site", "direction"]].head(1)
                        discarded_data = pd.concat([discarded_data, discard_record], ignore_index = True)
                        #table_number += 1
                        continue
                    
                    day_df = day_df.reset_index()
                    day_df = day_df.rename(columns={"index":"date"})

                    formatdf = pd.concat([formatdf,day_df], ignore_index = True)

                    formatdf['Source'] = 'ATC'
                    formatdf['flow_car'] = ''
                    formatdf['flow_LGV'] = ''
                    formatdf['flow_OGV_1'] = ''
                    formatdf['flow_OGV_2'] = ''
                    formatdf['flow_HGV'] = ''

                    #print(hourlydf)
                
            nan_value = float("NaN")

            formatdf.replace("", nan_value, inplace=True)

            formatdf.dropna(subset = ["flow_total"], inplace=True)

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


            survey_database = pd.concat([survey_database,formatdf], ignore_index = True)

    survey_database["Source"] = "HCC ATC"
    survey_database.to_csv(f"{output_dir}\\ATC_2023_HCC_NC_2.csv", index = False)
    discarded_data.to_csv(f"{discarded_dir}\\Discarded_Data_ATC_2023_HCC_NC_2.csv", index = False)


def format_ATC_2023_HCC_C_1(input_dir: str, output_dir: str, discarded_dir: str):
    os.chdir(input_dir)

    AM_PEAK_PERIOD = ["07:00", "8:00", "9:00"]
    PM_PEAK_PERIOD = ["16:00", "17:00", "18:00"]

    discarded_data = pd.DataFrame()

    files = glob.glob("*.csv")
    survey_database = pd.DataFrame()

    for file in files:
        print("Adding ",file," to the database")
        file_path = os.path.join(input_dir,file)

        table = 0
        i = 1
        
        for data in pd.read_csv(file_path, chunksize = 117):
            table = table + i
            site = str(data.columns.values[0])[-3:]

            if site == "001":
                    site = "611"
            else:
                    site = site[-3:]

            dir = data.iloc[1,6][9:]
            
            date = str(data.iloc[1,3])
            
            if dir == "Total Flow":
                continue
            
            data = data.rename(columns = data.iloc[4])
            data = data[5:101]
            data[['Car/lVan', 'Cr/lV+Tr', 'H. Van','LGV', 'Rigid', 'Rg+Tr', 'ArticHGV', 'Minibus', 'Bus' ]] = data[['Car/lVan', 'Cr/lV+Tr', 'H. Van','LGV', 'Rigid', 'Rg+Tr', 'ArticHGV', 'Minibus', 'Bus' ]].apply(pd.to_numeric)
            data['Date'] = date

            data['date'] = data['Date'] + " " + data['Begin']
            
            data['date'] = pd.to_datetime(data['date'])
            
            data['direction'] = dir
            data['site'] = site

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

            data['flow_car'] = data['Car/lVan'] + data['Cr/lV+Tr']
            data['flow_LGV'] = data['H. Van'] + data['LGV']
            data['flow_OGV_1'] = data['Rigid'] 
            data['flow_OGV_2'] = data['Rg+Tr'] + data['ArticHGV']
            data['flow_HGV'] = data['flow_OGV_1'] + data['flow_OGV_2']
            data['flow_BUS'] = data['Minibus'] + data['Bus']
            data['flow_total'] = data['flow_car'] + data['flow_LGV'] +  data['flow_HGV'] + data['flow_BUS']
            
            data = data.drop(data.columns[[0,1,2,3,4,5,6,7,8,9,10,11,12,]],axis = 1)

            data = data.set_index(["date"])

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

            nan_value = float("NaN")

            data['Source'] = 'ATC'

            data.replace("", nan_value, inplace = True)

            data.dropna(subset = ["flow_total"], inplace = True)

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

            data = data.reset_index()
            data = data.rename(columns = {"index": "date"})

            survey_database = pd.concat([survey_database,data], ignore_index = True)

    survey_database = survey_database.drop(columns = ['Date'])
    survey_database["Source"] = "HCC ATC"
    survey_database.to_csv(f"{output_dir}\\ATC_2023_HCC_C_1.csv", index = False)
    discarded_data.to_csv(f"{discarded_dir}\\Discarded_Data_ATC_2023_HCC_C_1.csv", index=False)

def format_ATC_2023_HCC_C_2(input_dir: str, output_dir: str, discarded_dir: str):
    os.chdir(input_dir)

    AM_PEAK_PERIOD = ["07:00", "8:00", "9:00"]
    PM_PEAK_PERIOD = ["16:00", "17:00", "18:00"]

    discarded_data = pd.DataFrame()

    #Time_CSV = pd.read_csv("Time.csv")
    files = glob.glob("*.csv")
    survey_database = pd.DataFrame()

    for file in files:
        print("Adding ",file," to the database")
        file_path = os.path.join(input_dir,file)

        table = 0
        i = 1
        n = -1
        
        for data in pd.read_csv(file_path, chunksize = 117):
            table = table + i
            site = str(data.columns.values[0])[-3:]
            print ("Table number : ", table, "   Site number :  ", site)
            #print (sheet_names)
            if site == "001":
                    site = "611"
            else:
                    site = site[-3:]

            dir = data.iloc[1,6][9:]
            
            date = str(data.iloc[1,3])
            
            #if len(date) == 0:
                #break
            if dir == "Total Flow":
                continue
                
                #"Southbound" "Eastbound" "Westbound" 
                    #continue
            #if dir == "Westbound OS" or dir == "Westbound NS" or dir == "Northbound 1" or dir == "Northbound 2" or dir == "Southbound 1" or dir == "Southbound 2" or dir == "Southbound 3":
                #continue
            #if dir == "Southwest 1" or dir == "Southwest 2" or dir == "Southeast 1" or dir == "Southeast 2" or dir == "Northwest 1" or dir == "Northwest 2" or dir == "Northeast 1" or dir == "Northeast 2" or dir == "Total Flow":
                #continue
            
            data = data.rename(columns = data.iloc[4])
            data = data[5:101]
            data[['Car/lVan', 'Cr/lV+Tr', 'H. Van','LGV', 'Rigid', 'Rg+Tr', 'ArticHGV', 'Minibus', 'Bus' ]] = data[['Car/lVan', 'Cr/lV+Tr', 'H. Van','LGV', 'Rigid', 'Rg+Tr', 'ArticHGV', 'Minibus', 'Bus' ]].apply(pd.to_numeric)
            data['Date'] = date

            data['date'] = data['Date'] + " " + data['Begin']
            
            data['date'] = pd.to_datetime(data['date'])
            
            data['direction'] = dir
            data['site'] = site

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

            data['flow_car'] = data['Car/lVan'] + data['Cr/lV+Tr']
            data['flow_LGV'] = data['H. Van'] + data['LGV']
            data['flow_OGV_1'] = data['Rigid'] 
            data['flow_OGV_2'] = data['Rg+Tr'] + data['ArticHGV']
            data['flow_HGV'] = data['flow_OGV_1'] + data['flow_OGV_2']
            data['flow_BUS'] = data['Minibus'] + data['Bus']
            data['flow_total'] = data['flow_car'] + data['flow_LGV'] +  data['flow_HGV'] + data['flow_BUS']

            data = data.drop(data.columns[[0,1,2,3,4,5,6,7,8,9,10,11,12,]],axis = 1)

            data = data.set_index(["date"])

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

            nan_value = float("NaN")

            data['Source'] = 'HCC ATC'

            data.replace("", nan_value, inplace=True)

            data.dropna(subset = ["flow_total"], inplace=True)

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

            data = data.reset_index()
            data = data.rename(columns = {"index": "date"})

            survey_database = pd.concat([survey_database,data], ignore_index = True)

            #if site == "349":
                #survey_database.drop(survey_database.columns['Date'])
            
                #rename first column to Total. Keep up to col N, and column U
                    
                #df = df.rename(columns={'Unnamed: 0':'tt'})

            #df['flow_car'] = df['flow_car1'].rolling(4).sum().shift(periods = -3)
            #df['flow_LGV'] = df['flow_LGV1'].rolling(4).sum().shift(periods = -3)
            #df['flow_OGV1'] = df['flow_OGV'].rolling(4).sum().shift(periods = -3)
            #df['flow_OGV2'] = df['flow_ogv3'].rolling(4).sum().shift(periods = -3)
            #df['flow_HGV'] = df['flow_HGV1'].rolling(4).sum().shift(periods = -3)
            #df['flow_PSV'] = df['flow_BUS'].rolling(4).sum().shift(periods = -3)
            #df['flow_total'] = df['flow_total1'].rolling(4).sum().shift(periods = -3)
            
            #print(data)

            #print(survey_database)

    

    survey_database["Source"] = "HCC ATC"
    survey_database.to_csv(f"{output_dir}\\ATC_2023_HCC_C_2.csv", index = False)
    discarded_data.to_csv(f"{discarded_dir}\\Discarded_Data_ATC_2023_HCC_C_2.csv", index= False)

def ATC_HCC_C_1_2(input_dir: str, output_dir: str, discarded_dir: str, year: int):
    os.chdir(input_dir)

    AM_PEAK_PERIOD = ["07:00", "8:00", "9:00"]
    PM_PEAK_PERIOD = ["16:00", "17:00", "18:00"]

    discarded_data = pd.DataFrame()

    #Time_CSV = pd.read_csv("Time.csv")
    files = glob.glob("*.csv")
    survey_database = pd.DataFrame()

    for file in files:
        print("Adding ",file," to the database")
        file_path = os.path.join(input_dir,file)

        table = 0
        i = 1
        n = -1
        
        for data in pd.read_csv(file_path, chunksize = 117):
            table = table + i
            site = str(data.columns.values[0])[-3:]
            print ("Table number : ", table, "   Site number :  ", site)
            #print (sheet_names)
            if site == "001":
                    site = "611"
            else:
                    site = site[-3:]

            dir = data.iloc[1,6][9:]
            
            date = str(data.iloc[1,3])
            if dir == "North" or dir == "South" or dir == "East" or dir =="West" or dir == "Northbound " or dir == "Southbound " or dir == "Eastbound " or dir == "Westbound ":
                data = data.rename(columns = data.iloc[4])
                data = data[5:101]
                data[['Car/lVan', 'Cr/lV+Tr', 'H. Van','LGV', 'Rigid', 'Rg+Tr', 'ArticHGV', 'Minibus', 'Bus' ]] = data[['Car/lVan', 'Cr/lV+Tr', 'H. Van','LGV', 'Rigid', 'Rg+Tr', 'ArticHGV', 'Minibus', 'Bus' ]].apply(pd.to_numeric)
                data['Date'] = date

                data['date'] = data['Date'] + " " + data['Begin']
                
                data['date'] = pd.to_datetime(data['date'])
                
                data['direction'] = dir
                data['site'] = site

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

                data['flow_car'] = data['Car/lVan'] + data['Cr/lV+Tr']
                data['flow_LGV'] = data['H. Van'] + data['LGV']
                data['flow_OGV_1'] = data['Rigid'] 
                data['flow_OGV_2'] = data['Rg+Tr'] + data['ArticHGV']
                data['flow_HGV'] = data['flow_OGV_1'] + data['flow_OGV_2']
                data['flow_BUS'] = data['Minibus'] + data['Bus']
                data['flow_total'] = data['flow_car'] + data['flow_LGV'] +  data['flow_HGV'] + data['flow_BUS']
                
                data = data.drop(data.columns[[0,1,2,3,4,5,6,7,8,9,10,11,12,]],axis = 1)

                data = data.set_index(["date"])

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

                nan_value = float("NaN")

                data['Source'] = 'HCC ATC'

                data.replace("", nan_value, inplace=True)

                data.dropna(subset = ["flow_total"], inplace=True)

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

                data = data.reset_index()
                data = data.rename(columns = {"index": "date"})

                survey_database = pd.concat([survey_database,data], ignore_index = True)
    
    #survey_database = survey_database.drop(columns = ["Date"])
    survey_database["Source"] = "HCC ATC"
    survey_database.to_csv(f"{output_dir}\\ATC_{str(year)}_C_1_2.csv", index = False)
    discarded_data.to_csv(f"{discarded_dir}\\Discarded_Data_ATC_{str(year)}_HCC_C_1_2.csv", index= False)