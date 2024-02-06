"""
Author: Thomas Butler

Contributors:
"""

import geopandas as gpd
import pandas as pd
from shapely.wkt import loads
from pathlib import Path
import logging
import os

#INPUTS
PROGRAM_DIRECTORY = os.getcwd()
SHAPE_FILES_DIRECTORY = f"{PROGRAM_DIRECTORY}\\Shape Files"
STUDY_AREA_FILE_NAME = 'Study_Area_Zones_with_UrbanRural_Classification'

logger = logging.getLogger(__name__)

def define_urban_rural(row):
    """
    Function that sorts zones into rural and urban zones
    """
    if "Rural" in row["RUC11"]:
        row["Urban/Rural"] = "Rural"
    else:
        row["Urban/Rural"] = "Urban"
    return row

def user_class_filter(data):
    """
    Select for user classes that are relevant to study
    """

    data["User Class"] = data["User Class"].astype(int)
    user_class_filter_data = data[data['User Class'] <= 3]
    user_class_filter_data = user_class_filter_data.drop(columns="User Class")
    return user_class_filter_data

def distance_filter(data, distance_buffer_threshold):
    """
    Only select trips that travel less than 150km (possible by PT)
    """
    if distance_buffer_threshold is None:
        logger.warning("Distance filter applied with no specified distance threshold. Filter will do nothing.")
        return data
    else:
        distance_filter_data = data[data["length"] < distance_buffer_threshold]
        return distance_filter_data

def filter_zero_trips_pairs(data):
    """
    Select OD paris that have a non-zero demand
    """

    non_zero_trip_data = data[data['Total Demand'] > 0]
    return non_zero_trip_data

def filter_study_area_pairs(data, model_zones_set, port_zones):
    """
    Select study trips that start or end in the study area 
    """

    #Need to split dataframe to stop length and coordinates being summed
    split_data = data[["Origin", "Destination", "Total Demand", "vkt"]].copy()

    #Sum together user classe trips and set the index to Origin and Destination
    split_data = split_data.groupby(['Origin', 'Destination']).sum().reset_index()

    #Store coordinates and length separately
    coords = data[["Origin", "Destination","Origin_x", "Origin_y", "Dest_x", "Dest_y", "length"]]
    coords.drop_duplicates(inplace=True)

    #Recombine summed data with length and coordinate information
    merged_data = pd.merge(split_data, coords, how='left', left_on=["Origin", "Destination"], right_on=["Origin", "Destination"])
    merged_data = merged_data.set_index(["Origin", "Destination"])

    #Check if both or either of the origin or destination are in the study area and produce a list that tracks this
    study_area = merged_data.apply(lambda x: x.name[0] in model_zones_set or x.name[1] in model_zones_set, axis=1)
   
    #Extract trips that fall inside and outside the study area
    study_area_trips = merged_data[study_area]
    study_area_trips = study_area_trips.reset_index()

    if port_zones is None:
        logger.warning("Study area filtering completed with no port zones specified.")
    else:
        #Remove port zones
        for port in port_zones:
            study_area_trips = study_area_trips[~(study_area_trips["Origin"] == port)]
            study_area_trips = study_area_trips[~(study_area_trips["Destination"] == port)]

    return study_area_trips

def classify_urban_rural_pairs(data, model_zones):
    """
    Creates an urban/rural lookup table and adds an urban/rural classification column to the dataframe
    """
    #Creates an urban/rural lookup table to sort the data for processing
    urban_rural_lookup = model_zones[["Zone_ID", "RUC11"]]
    urban_rural_lookup = urban_rural_lookup.apply(define_urban_rural, axis=1).drop(columns=["RUC11"])
    rural_zones = urban_rural_lookup[urban_rural_lookup["Urban/Rural"] == "Rural"]
    rural_zones = set(rural_zones["Zone_ID"])
    urban_rural_lookup.to_csv(f"urban_rural_lookup.csv")

    data.set_index(["Origin", "Destination"], inplace=True)

    #Check if both or either of the origin or destination are rural and produce a list that tracks this
    rural_trips = data.apply(lambda x: x.name[0] in rural_zones or x.name[1] in rural_zones, axis=1)

    #Classify the OD pairs into urban or rural
    rural_data = data[rural_trips]
    urban_data = data[~rural_trips]
    rural_data["Urban/Rural"] = "Rural"
    urban_data["Urban/Rural"] = "Urban"
    classified_data = pd.concat([rural_data, urban_data])
    classified_data.reset_index(inplace=True)
    classified_data = classified_data.sort_values('Origin')   

    return classified_data

def filter_intrazonal_pairs(data, OUTPUT_DIRECTORY):
    """
    Extracts the intrazonal trips
    """

    internal_trips = data[data["Origin"] == data["Destination"]]
    internal_trips.to_csv(f"{OUTPUT_DIRECTORY}\\internal_trips.csv", index=False)

    non_internal_data = data[~(data["Origin"] == data["Destination"])]

    return non_internal_data



def filtering_model_outputs(filter_distance: bool, 
                            filter_user_class: bool = True, 
                            filter_zero_trips: bool = True,
                            filter_study_area: bool = True,
                            classify_urban_rural: bool = True,
                            filter_internal_zones: bool = True,
                            port_zones: list() = None,
                            distance_buffer_threshold: int = None):
    """
    Function that takes processed model outputs and filters them to a reasonable number to run through the Google API.

    Bool inputs select which filtering steps to execute, order can be changed using cut and paste. All set to run as default, 
    except for the distance threshold filter which is only nessecary for larger projects.

    Port zones input should be a list of integers that refer to Zone ID of port zones. Distance buffer threshold should be an
    integer value in km. 
    """

    fmt = "{asctime} [{name:40.40}] [{levelname:^10.10}] {message}"
    logging.basicConfig(level=logging.INFO, format=fmt, style="{")

    # Create folder to store API outputs
    cwd = Path.cwd()
    path = Path.joinpath(cwd, "Outputs")
    Path(path).mkdir(parents=True, exist_ok=True)

    path = Path.joinpath(cwd, "Outputs\\Filter stats")
    Path(path).mkdir(parents=True, exist_ok=True)

    OUTPUT_DIRECTORY = f"{PROGRAM_DIRECTORY}\\Outputs"

    logger.info("Loading processed model outputs")

    data = pd.read_csv("all_model_pairs.csv")

    #Read in shapefile of study area zones and extract zone IDs
    model_zones = gpd.read_file(f"{SHAPE_FILES_DIRECTORY}\\{STUDY_AREA_FILE_NAME}.shp")
    model_zones = pd.DataFrame(model_zones).drop(columns=["OBJECTID", "CenX", "CenY", "FIRST_Mode", "Area", "PopX", "PopY", "Shape_Leng", "Radius", "GRID_ID"])
    model_zones_set = set(model_zones["Zone_ID"])

    #Dataframe to store the number of OD pairs, trips and vkt after each filter
    filter_stats = pd.DataFrame()
    perc = '%'

    logger.info("Loading geometry onto outputs")
    #Convert the data to a geopandas dataframe so the distance between OD pairs can be found
    data['geometry'] = data['wkt_geometry'].apply(lambda x: loads(x))
    data = gpd.GeoDataFrame(data.drop('wkt_geometry',axis=1), crs='EPSG:27700')

    logger.info("Finding geometry length")
    #Calculate length of straight lines between OD pairs
    data["length"] = data.geometry.length

    #Convert to km
    data["length"] = data["length"] / 1000

    #Calculate vehicle kilometres
    data["vkt"] = data["length"] * data["Total Demand"]

    #Calculate statistics about OD pairs before filtering begins
    initial_pairs = len(data.index)
    initial_trips = data['Total Demand'].sum()
    initial_vkt = data['vkt'].sum()

    #Store stats before filtering
    filter_stats = pd.concat([filter_stats, pd.DataFrame({"Step": "Initial", "OD Pairs": [initial_pairs], f"{perc} of total pairs": [(initial_pairs / initial_pairs)], "Trips": [initial_trips], f"{perc} of total trips": [(initial_trips / initial_trips)], "VKT": [initial_vkt], f"{perc} of total vkt": [(initial_vkt / initial_vkt)]})])
    
    logger.info(f"Initial pairs: {initial_pairs}")
    logger.info(f"Initial trips: {initial_trips}")
    logger.info(f"Initial vkt: {initial_vkt}")

    if filter_user_class == True:
        data = user_class_filter(data)
        pairs_after_user_class_filter = len(data.index)
        trips_after_user_class_filter = data['Total Demand'].sum()
        vkt_after_user_class_filter = data['vkt'].sum()

        filter_stats = pd.concat([filter_stats, pd.DataFrame({"Step": "User class filter", "OD Pairs": [pairs_after_user_class_filter], f"{perc} of total pairs": [(pairs_after_user_class_filter / initial_pairs)], "Trips": [trips_after_user_class_filter], f"{perc} of total trips": [(trips_after_user_class_filter / initial_trips)], "VKT": [vkt_after_user_class_filter], f"{perc} of total vkt": [(vkt_after_user_class_filter / initial_vkt)]})])
    
        logger.info(f"Pairs after user class filter: {pairs_after_user_class_filter}")
        logger.info(f"Trips after user class filter: {trips_after_user_class_filter}")
        logger.info(f"VKT after user class filter: {vkt_after_user_class_filter}")

    if filter_distance == True:
        data = distance_filter(data, distance_buffer_threshold)

        pairs_after_distance_filter = len(data.index)
        trips_after_distance_filter = data['Total Demand'].sum()
        vkt_after_distance_filter = data['vkt'].sum()

        filter_stats = pd.concat([filter_stats, pd.DataFrame({"Step": "150km buffer", "OD Pairs": [pairs_after_distance_filter], f"{perc} of total pairs": [(pairs_after_distance_filter / initial_pairs)], "Trips": [trips_after_distance_filter], f"{perc} of total trips": [(trips_after_distance_filter / initial_trips)], "VKT": [vkt_after_distance_filter], f"{perc} of total vkt": [(vkt_after_distance_filter / initial_vkt)]})])
        
        logger.info(f"Pairs after long distance filter: {pairs_after_distance_filter}")
        logger.info(f"Trips after long distance filter: {trips_after_distance_filter}")
        logger.info(f"VKT after long distance filter: {vkt_after_distance_filter}")
    
    if filter_zero_trips == True:
        data = filter_zero_trips_pairs(data)
    
        pairs_after_zero_trip_filter = len(data.index)
        trips_after_zero_trip_filter = data['Total Demand'].sum()
        vkt_after_zero_trip_filter = data['vkt'].sum()

        filter_stats = pd.concat([filter_stats, pd.DataFrame({"Step": "Zero trip filter", "OD Pairs": [pairs_after_zero_trip_filter], f"{perc} of total pairs": [(pairs_after_zero_trip_filter / initial_pairs)], "Trips": [trips_after_zero_trip_filter], f"{perc} of total trips": [(trips_after_zero_trip_filter / initial_trips)], "VKT": [vkt_after_zero_trip_filter], f"{perc} of total vkt": [(vkt_after_zero_trip_filter / initial_vkt)]})])

        logger.info(f"Pairs after zero trip filter: {pairs_after_zero_trip_filter}")
        logger.info(f"Trips after zero trip filter: {trips_after_zero_trip_filter}")
        logger.info(f"VKT after zero trip filter: {vkt_after_zero_trip_filter}")
    
    if filter_study_area == True:
        data = filter_study_area_pairs(data, model_zones_set, port_zones)

        study_area_pairs = len(data.index)
        study_area_trips_total = data['Total Demand'].sum()
        study_area_vkt = data['vkt'].sum()

        filter_stats = pd.concat([filter_stats, pd.DataFrame({"Step": "Study area filter", "OD Pairs": [study_area_pairs], f"{perc} of total pairs": (study_area_pairs / initial_pairs), "Trips": [study_area_trips_total], f"{perc} of total trips": [(study_area_trips_total / initial_trips)], "VKT": [study_area_vkt], f"{perc} of total vkt": [(study_area_vkt / initial_vkt)]})])
        
        logger.info(f"Pairs after study area filter: {study_area_pairs}")
        logger.info(f"Trips after study area filter: {study_area_trips_total}")
        logger.info(f"VKT after study area filter: {study_area_vkt}")

    if classify_urban_rural == True:
        data = classify_urban_rural_pairs(data, model_zones)

    if filter_internal_zones == True:
        data = filter_intrazonal_pairs(data, OUTPUT_DIRECTORY)
        pairs_after_internal_filter = len(data.index)
        trips_after_internal_filter = data['Total Demand'].sum()
        vkt_after_internal_filter = data['vkt'].sum()

        filter_stats = pd.concat([filter_stats, pd.DataFrame({"Step": "Internal zones removed", "OD Pairs": [pairs_after_internal_filter], f"{perc} of total pairs": [(pairs_after_internal_filter / initial_pairs)], "Trips": [trips_after_internal_filter], f"{perc} of total trips": [(trips_after_internal_filter / initial_trips)], "VKT": [vkt_after_internal_filter], f"{perc} of total vkt": [(vkt_after_internal_filter / initial_vkt)]})])

        logger.info(f"Pairs after intrazonal filter: {pairs_after_internal_filter}")
        logger.info(f"Trips after intrazonal filter: {trips_after_internal_filter}")
        logger.info(f"VKT after intrazonal filter: {vkt_after_internal_filter}")

    data.to_csv("processed_model_outputs.csv", index=False)
    filter_stats.to_csv(f"{OUTPUT_DIRECTORY}\\Filter stats\\model_filter_stats.csv", index=False)
    
