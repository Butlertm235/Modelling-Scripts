import pandas as pd
import os
import File_directories
import glob
import datetime


# Define datetime intervals for AM, IP, PM and OP intervals
AM_HOURLY_INTERVALS = [datetime.time(hour = 7, minute = 00, second = 0),datetime.time(hour = 8, minute = 00, second = 0),datetime.time(hour = 9, minute = 00, second = 0)]
IP_HOURLY_INTERVALS = [datetime.time(hour = 10, minute = 0, second = 0),datetime.time(hour = 11, minute = 0, second = 0),datetime.time(hour = 12, minute = 0, second = 0),datetime.time(hour = 13, minute = 0, second = 0),datetime.time(hour = 14, minute = 0, second = 0),datetime.time(hour = 15, minute = 0, second = 0)]
PM_HOURLY_INTERVALS = [datetime.time(hour = 16, minute = 00, second = 0),datetime.time(hour = 17, minute = 00, second = 0),datetime.time(hour = 18, minute = 00, second = 0)]
OP_HOURLY_INTERVALS = [datetime.time(hour = 19, minute = 00, second = 0),datetime.time(hour = 20, minute = 00, second = 0),datetime.time(hour = 21, minute = 00, second = 0), datetime.time(hour = 22, minute = 00, second = 0),datetime.time(hour = 23, minute = 00, second = 0),datetime.time(hour = 00, minute = 00, second = 0),datetime.time(hour = 1, minute = 00, second = 0),datetime.time(hour = 2, minute = 00, second = 0),datetime.time(hour = 3, minute = 00, second = 0),datetime.time(hour = 4, minute = 00, second = 0),datetime.time(hour = 5, minute = 00, second = 0),datetime.time(hour = 6, minute = 00, second = 0)]



TIME_PERIODS = {"AM": AM_HOURLY_INTERVALS,
                "IP": IP_HOURLY_INTERVALS,
                "PM": PM_HOURLY_INTERVALS,
                "OP": OP_HOURLY_INTERVALS,
                }


# Function required for trip distance distribution
def distance_bander(row):
    
    distance = row["Trip_distance_km"]
    if distance <= 5:
        row["distance_band"] = 0
    elif distance <= 10:
        row["distance_band"] = 1
    elif distance <= 15:
        row["distance_band"] = 2
    elif distance <= 20:
        row["distance_band"] = 3
    elif distance <= 25:
        row["distance_band"] = 4
    elif distance <= 30:
        row["distance_band"] = 5
    elif distance <= 35:
        row["distance_band"] = 6
    elif distance <= 40:
        row["distance_band"] = 7
    elif distance <= 45:
        row["distance_band"] = 8
    elif distance <= 50:
        row["distance_band"] = 9
    elif distance <= 75:
        row["distance_band"] = 10
    elif distance <= 100:
        row["distance_band"] = 11
    elif distance <= 200:
        row["distance_band"] = 12
    elif distance <= 300:
        row["distance_band"] = 13
    elif distance <= 400:
        row["distance_band"] = 14
    elif distance <= 500:
        row["distance_band"] = 15
    elif distance <= 750:
        row["distance_band"] = 16
    elif distance <= 999:
        row["distance_band"] = 17
    else:
        row["distance_band"] = 18

    return row


def combine_INRIX_data(input_dir: str):
    """
    Combine all files in a given directory, returning one combined dataframe
    """
    
    os.chdir(input_dir)

    files = glob.glob("*.csv")

    output_df = pd.DataFrame()
    for file in files:
        data = pd.read_csv(file)

        output_df = pd.concat([output_df,data])
    
    return output_df


def organise_df(data: pd.DataFrame, zone_lookup: pd.DataFrame):
    """
    Takes combined dataset and performs the following steps:

        - Remove duplicates, printing entry numbers before and after
        - reformat starting and finishing dates and times for trips
        - prints the number of weekend and weekday entries before filtering to include weekday only
        - classify trips as AM, IP, PM or OP based on trip start time
        -Include MDN origin and destination zones by merging, requiring the zone lookup as an input of type pd.DataFrame
    """
    # Remove duplicates
    print("Entries before dropping duplicates", len(data["Year"]))

    data.drop_duplicates(subset = ["TripID"], keep = "first", inplace = True)

    print("Entries after dropping duplicates", len(data["Year"]))


    # Format dates and times
    data["OriginDateTime"] = data["OriginDateTime"].map(lambda x: x.rstrip(" UTC"))
    data["DestinationDateTime"] = data["DestinationDateTime"].map(lambda x: x.rstrip(" UTC"))

    data["OriginDateTime"] = pd.to_datetime(data["OriginDateTime"], dayfirst = True)
    data["DestinationDateTime"] = pd.to_datetime(data["DestinationDateTime"], dayfirst = True)

    data["OriginTime"] = data["OriginDateTime"].dt.time
    data["DestTime"] = data["DestinationDateTime"].dt.time


    data["OriginDate"] = data["OriginDateTime"].dt.date
    data["DestDate"] = data["DestinationDateTime"].dt.date

    data["OriginMonth"] = data["OriginDateTime"].dt.month
    data["DestMonth"] = data["DestinationDateTime"].dt.month

    data["Origin_DOW"] = data["OriginDateTime"].dt.dayofweek
    data["Dest_DOW"] = data["DestinationDateTime"].dt.dayofweek

    # Filter to isolate weekday data
    print("weekday data entries: ", len(data.loc[data["Origin_DOW"]<5]))
    print("weekend data entries: ", len(data.loc[data["Origin_DOW"]>4]))

    weekday_data = data.loc[data["Origin_DOW"] < 5]

    # AM, PM, IP, OP classification
    np_hour_convert = lambda x: datetime.datetime.strptime(x[0:2]+":00:00",  "%H:00:00")
    weekday_data["starting_hour"] = weekday_data["OriginTime"].astype(str)
    weekday_data["starting_hour"] = weekday_data["starting_hour"].apply(np_hour_convert)
    weekday_data["starting_hour"] = weekday_data["starting_hour"].dt.time
    

    data_AM = weekday_data.loc[weekday_data["starting_hour"].isin(TIME_PERIODS["AM"])]
    data_AM["time_period"] = "AM"
    data_IP = weekday_data.loc[weekday_data["starting_hour"].isin(TIME_PERIODS["IP"])]
    data_IP["time_period"] = "IP"
    data_PM = weekday_data.loc[weekday_data["starting_hour"].isin(TIME_PERIODS["PM"])]
    data_PM["time_period"] = "PM"
    data_OP = weekday_data.loc[weekday_data["starting_hour"].isin(TIME_PERIODS["OP"])]
    data_OP["time_period"] = "OP"


    output = pd.concat([data_AM, data_IP, data_PM, data_OP])


    # Merge to include MND zones
    output = pd.merge(output, zone_lookup, how = "inner", left_on = "OriginZone", right_on = "LSOA11CD")
    output = output.rename(columns = {"OBJECTID": "MND_Zone_Origin", "Study Area": "Study_Area_Origin", "Hertfordshire" : "Hertfordshire_Origin", "Region":"Region_Origin"})

    output = pd.merge(output, zone_lookup, how = "inner", left_on = "DestinationZone", right_on = "LSOA11CD")
    output = output.rename(columns = {"OBJECTID": "MND_Zone_Destination", "Study Area": "Study_Area_Destination", "Hertfordshire" : "Hertfordshire_Destination", "Region":"Region_Destination"})

    output[["Study_Area_Origin", "Hertfordshire_Origin", "Study_Area_Destination", "Hertfordshire_Destination"]] = output[["Study_Area_Origin", "Hertfordshire_Origin", "Study_Area_Destination", "Hertfordshire_Destination"]].fillna(0)

    return output


def trip_length_distribution(data: pd.DataFrame):
    """
    Output the trip length distribution as a csv file

    Only requires the formatted data to be passed in as a dataframe with the column names referenced
    """
    
    # Only use trip distance and vehicle type columns
    data = data[[ "trip_distance_m", "vehicle_type"]]

    # Express trip distance in km
    data = data.rename(columns = {"trip_distance_m": "Trip_distance_km"})

    data["Trip_distance_km"] = data["Trip_distance_km"] / 1000.0

    # Introduce distance bands in the dataframe and count entries in each band for each vehicle type
    data = data.apply(distance_bander, axis = 1)

    data = data.groupby(["distance_band", "vehicle_type"]).count()

    data = data.reset_index()

    # Reformat into a pivot table with vehicle classes as columns
    data = pd.pivot(data, index = "distance_band", columns = "vehicle_type")

    # Output to csv
    data.to_csv(f"{File_directories.PROGRAM_DIRECTORY}\\INRIX OD Data\\Results\\INRIX_OD_Trips_distance_band.csv")


def int_ext_matrix(data: pd.DataFrame, tp: str, UC: str):
    """
    Produces a sector based matrix based on a variable in the given dataframe
    Used to differentiate detailed modelled area and fully modelled area boundaries
    Region_Origin or Region_Destination are fields with a value between 0 and 2 representing
    zones outisde the study area, inside the study area but outside the Hertfordshire boundary,
    and those inside the Hertfordshire boundary

    This function produces the matrix for a specified time period and userclass
    """

    # Filter data by time period and user class
    if UC == "ALL":
        data = data.loc[(data["time_period"] == tp)]
    else:
        data = data.loc[(data["time_period"] == tp) & (data["vehicle_type"] == UC)]

    # Count region origin and destination occurances
    data = data.groupby(["Region_Origin", "Region_Destination"]).count()

    # Reformat into a pivot table
    table = data.pivot_table(index = "Region_Origin", columns = "Region_Destination", values = "TripID")

    # Output to csv
    table.to_csv(f"{File_directories.PROGRAM_DIRECTORY}\\INRIX OD Data\\Results\\INRIX_int_ext_matrix_{tp}_{UC}.csv")


def create_int_ext_matrices(data: pd.DataFrame):
    """
    Creates internal and external matrix for each time period and vehicle class
    """
    for tp in ["AM","IP","PM", "OP"]:
        for vc in ["CAR","LGV","HGV", "ALL"]:
            int_ext_matrix(data,tp,vc)




def MPZ_OD_Matrix(input_data: pd.DataFrame, time_period: str, vehicle_class: str, format: str = "grid"):
    """
    Produces three different types of trip matrices for the OD data for a particular time period and vehicle class,
    either in grid format or list format:

    1. Matrix containing all demand across the full sample of dates
    2. Matrix containing all demand divided by total number of days
    3. Matrix containing demand with trips weighted by vehicle ID occurance proportion of the number of the days

    """

    
    # Record total number of dates in sample
    number_of_days = len(input_data["Date"].unique())

    # Filter by time period
    data_tp = input_data.loc[input_data["time_period"] == time_period]

    # Count trips for each vehicle ID for the specified time period
    tp_vehid_trips = data_tp.groupby(["VehicleID"]).count()
    tp_vehid_trips = tp_vehid_trips.reset_index()
    tp_vehid_trips = tp_vehid_trips[["VehicleID" , "TripID"]]
    tp_vehid_trips = tp_vehid_trips.rename(columns = {"TripID" : "Vehicle_ID_Trips"})

    # Count trips for each OD pair for specified UC
    if vehicle_class == "ALL":
        data = data_tp.groupby(["MND_Zone_Origin", "MND_Zone_Destination"]).count().unstack(fill_value=0).stack()
        data = data.reset_index()
    else:
        data = data_tp.loc[data_tp["vehicle_type"] == vehicle_class]
        data = data.groupby(["MND_Zone_Origin", "MND_Zone_Destination"]).count().unstack(fill_value=0).stack()
        data = data.reset_index()

    
    if vehicle_class != "ALL":
        # Filter by vehicle class
        data_v = data_tp.loc[data_tp["vehicle_type"] == vehicle_class]

    # Count trips specific to vehicle ID for each OD pair
    data_v = data_tp.groupby(["MND_Zone_Origin", "MND_Zone_Destination", "VehicleID"]).count()
    data_v = data_v.reset_index()

    data_v = pd.merge(data_v, tp_vehid_trips, on = "VehicleID", how = "inner")
    # Calculate weight based on proportion of total days the vehicle is used
    data_v["VC_weight"] = data_v["Date"] / data_v["Vehicle_ID_Trips"]

    # Sum counts for each OD
    data_v = data_v.groupby(["MND_Zone_Origin", "MND_Zone_Destination"]).sum().unstack(fill_value=0).stack()
    data_v = data_v.reset_index()


    # Output three described matrices in either grid format or list format
    if format == "grid":

        #### Note: Grid format will not include zones with zero origin or destination trips in the corresponding axes
        matrix = data.pivot_table(index = "MND_Zone_Origin", columns = "MND_Zone_Destination", values = "TripID")

        matrix_avg_over_total_days = matrix / number_of_days

        matrix_vehid = data_v.pivot_table(index = "MND_Zone_Origin", columns = "MND_Zone_Destination", values = "VC_weight")
        
        matrix.to_csv(f"{File_directories.PROGRAM_DIRECTORY}\\INRIX OD Data\\Results\\Matrices\\Total Counts\\MND_OD_Matrix_UC_{vehicle_class}_{time_period}_Total_Count.csv")

        matrix_avg_over_total_days.to_csv(f"{File_directories.PROGRAM_DIRECTORY}\\INRIX OD Data\\Results\\Matrices\\Avg by total days\\MND_OD_Matrix_UC_{vehicle_class}_{time_period}_Avg_Count_Per_Day.csv")

        matrix_vehid.to_csv(f"{File_directories.PROGRAM_DIRECTORY}\\INRIX OD Data\\Results\\Matrices\\VehID_Weighted_by_days\\MND_OD_Matrix_UC_{vehicle_class}_{time_period}_Avg_Count_Weighted_by_VehID_Per_Day.csv")

    else:
        matrix = data[["MND_Zone_Origin", "MND_Zone_Destination", "TripID"]]
        matrix = matrix.rename(columns = {"MND_Zone_Origin": "O", "MND_Zone_Destination": "D", "TripID": "TRIP"})

        # Fill zero OD pairs
        zones = range(1,451)
        all_od = [(o, d) for o in zones for d in zones]
        all_od_df = pd.DataFrame(all_od, columns = ["O" , "D"])
        exp_matrix = pd.merge(all_od_df, matrix, on = ["O", "D"], how = "left")
        exp_matrix["TRIP"] = exp_matrix["TRIP"].fillna(0)


        exp_matrix.to_csv(f"{File_directories.PROGRAM_DIRECTORY}\\INRIX OD Data\\Results\\Matrices\\Total Counts\\MND_OD_Matrix_UC_{vehicle_class}_{time_period}_Total_Count.csv", index = False)

        matrix_avg_over_total_days = exp_matrix
        matrix_avg_over_total_days["TRIP"] = matrix_avg_over_total_days["TRIP"] / number_of_days

        matrix_vehid = data_v[["MND_Zone_Origin", "MND_Zone_Destination", "VC_weight"]]
        matrix_vehid = matrix_vehid.rename(columns = {"MND_Zone_Origin": "O", "MND_Zone_Destination": "D", "VC_weight": "TRIP"})

        exp_matrix_vehid = pd.merge(all_od_df, matrix_vehid, on = ["O", "D"], how = "left")
        exp_matrix_vehid["TRIP"] = exp_matrix_vehid["TRIP"].fillna(0)
        
        matrix_avg_over_total_days.to_csv(f"{File_directories.PROGRAM_DIRECTORY}\\INRIX OD Data\\Results\\Matrices\\Avg by total days\\MND_OD_Matrix_UC_{vehicle_class}_{time_period}_Avg_Count_Per_Day.csv", index = False)

        exp_matrix_vehid.to_csv(f"{File_directories.PROGRAM_DIRECTORY}\\INRIX OD Data\\Results\\Matrices\\VehID_Weighted_by_days\\MND_OD_Matrix_UC_{vehicle_class}_{time_period}_Avg_Count_Weighted_by_VehID_Per_Day.csv", index = False)
        


def create_matrices(data:pd.DataFrame):
    """
    Creates all OD trip matrices
    """

    for tp in ["AM","IP","PM", "OP"]:
        for vc in ["CAR","LGV","HGV", "ALL"]:
            print(f"Creating matrix {vc} {tp}")
            MPZ_OD_Matrix(data,tp,vc, "table")

if __name__ == "__main__":

    # Zone lookup comparing LSOA to MPZ zones
    LSOA_MPZ_lookup = pd.read_csv(f"{File_directories.PROGRAM_DIRECTORY}\\util\\INRIX_OD_Data\\LSOA_MND_Lookup.csv")

    # Combine the raw data files
    combined_data = combine_INRIX_data(f"{File_directories.PROGRAM_DIRECTORY}\\INRIX OD Data\\")
    
    # Format combined database
    cleaned_data = organise_df(combined_data, LSOA_MPZ_lookup)

    # Filter by neutral dates
    date_filter = pd.read_csv(f"{File_directories.PROGRAM_DIRECTORY}\\util\\INRIX_OD_Data\\Holiday_Filter.csv")
    date_filter["Date"] = pd.to_datetime(date_filter["Date"], dayfirst= True).dt.date
    cleaned_data = pd.merge(cleaned_data, date_filter, how = "inner", left_on = "OriginDate", right_on = "Date")

    # Check 1: Calculate trips by month, vehicle class, time period
    trips_by_month = cleaned_data.groupby(["OriginMonth", "time_period", "veh_cls"]).count()
    trips_by_month = trips_by_month.reset_index()
    trips_by_month.to_csv(f"{File_directories.PROGRAM_DIRECTORY}\\INRIX OD Data\\Results\\INRIX_OD_Trips_by_month.csv", index=False)

    # Check 2: Calculate trips by time period and vehicle class
    trips_by_tp = cleaned_data.groupby(["time_period", "veh_cls"]).count()
    trips_by_tp = trips_by_tp.reset_index()
    trips_by_tp.to_csv(f"{File_directories.PROGRAM_DIRECTORY}\\INRIX OD Data\\Results\\INRIX_OD_Trips_by_TP_VC.csv", index=False)
    
    # Attach vehicle type to database
    def update_veh(vehcls):
        if vehcls < 1.5 :
            return "CAR"
        elif vehcls < 2.5:
            return "LGV"
        else:
            return "HGV"

    cleaned_data["vehicle_type"] = cleaned_data["veh_cls"].apply(lambda x: update_veh(x))

    
    # Check unique records, by trip ID and vehicle ID
    print("Total records = ", len(cleaned_data["Year"]))
    print("Total records by unique trip ID = ", len(cleaned_data["TripID"].unique()))
    print("Total records by unique vehicle ID = ", len(cleaned_data["VehicleID"].unique()))
    
    # Check trips by UC
    print("Total car trips = ", len(cleaned_data.loc[cleaned_data["vehicle_type"] == "CAR"]["Year"]))
    print("Total LGV trips = ", len(cleaned_data.loc[cleaned_data["vehicle_type"] == "LGV"]["Year"]))
    print("Total HGV trips = ", len(cleaned_data.loc[cleaned_data["vehicle_type"] == "HGV"]["Year"]))

    # Check 3: Trips by vehicle type
    avgs_by_vehcls = cleaned_data.groupby(["vehicle_type"]).mean()
    avgs_by_vehcls = avgs_by_vehcls.reset_index()
    avgs_by_vehcls.to_csv(f"{File_directories.PROGRAM_DIRECTORY}\\INRIX OD Data\\Results\\average_trip_len_time_by_vehicle_class.csv", index=False)

    # Check intrazonal trips
    intrazonals = cleaned_data.loc[cleaned_data["OriginZone"] == cleaned_data["DestinationZone"]]
    print("Total Intrazonal trips = ", len(intrazonals["Year"]))

    # Output formatted inrix OD data
    cleaned_data.to_csv(f"{File_directories.PROGRAM_DIRECTORY}\\INRIX OD Data\\Results\\Weekday_Formatted_INRIX_data.csv", index = False)
    

    # Read checked data with sector specific origins and destinations added manually for int / ext matrix
    final_data = pd.read_csv(f"C:\\Users\\UKHRM004\\Documents\\HCC Model Update\\For Github\\INRIX OD Data\\Results\\INRIX_Final_Dataset.csv")

    # Output LGV trips by time period and vehicle ID
    trips_by_vehid_tp = final_data.groupby(["VehicleID", "time_period", "vehicle_type"]).count().reset_index()
    trips_by_vehid_tp = trips_by_vehid_tp.loc[trips_by_vehid_tp["vehicle_type"] == "LGV"]
    trips_by_vehid_tp = trips_by_vehid_tp.reset_index()[["VehicleID", "time_period", "TripID"]]
    trips_by_vehid_tp = trips_by_vehid_tp.rename(columns = {"TripID" : "TRIP"})
    trips_by_vehid_tp.to_csv(f"{File_directories.PROGRAM_DIRECTORY}\\INRIX OD Data\\Results\\LGV_Trips_by_Vehicle_ID_by_TP.csv", index = False)


    # Create trip length distribution
    trip_length_distribution(cleaned_data)

    # Create internal / external trip matrices for data checks
    create_int_ext_matrices(cleaned_data)
    
    # Create all OD matrices
    create_matrices(final_data)
    

    
