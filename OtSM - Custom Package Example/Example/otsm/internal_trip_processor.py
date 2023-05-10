"""
Author: Thomas Butler

Contributors:
"""

import numpy as np
import pandas as pd
import geopandas as gpd
import logging
import math
import os

logger = logging.getLogger(__name__)

def internal_processor(high_urban_active_travel_threshold, low_urban_active_travel_threshold, high_rural_active_travel_threshold, low_rural_active_travel_threshold):
    """
    Function that finds the estimated internal vkt and divides the trips into short (possible by active travel) and long.

    All inputs should be in km
    """

    PROGRAM_DIRECTORY = os.getcwd()
    SHAPE_FILES_DIRECTORY = f"{PROGRAM_DIRECTORY}\\Shape Files"
    INTERNAL_TRIPS_DIRECTORY = f"{PROGRAM_DIRECTORY}\\Outputs"
    OUTPUT_FOLDER = f"{INTERNAL_TRIPS_DIRECTORY}\\Analysed_Internals\\"
    pi = math.pi

    fmt = "{asctime} [{name:40.40}] [{levelname:^10.10}] {message}"
    logging.basicConfig(level=logging.INFO, format=fmt, style="{")

    logger.info("Beginning internal trips analysis")

    data = pd.read_csv(f"{INTERNAL_TRIPS_DIRECTORY}\\internal_trips.csv").drop(columns=["Destination","vkt","Origin_x","Origin_y","Dest_x","Dest_y","length"])

    study_area_zones = gpd.read_file(f"{SHAPE_FILES_DIRECTORY}\\Study_Area_Zones_with_UrbanRural_Classification.shp")
    study_area_zones = study_area_zones[["Zone_ID", "Area"]]

    #Longest lines file should be created using longest lines code which requires QGIS simplification of the zones
    longest_lines = gpd.read_file(f"{SHAPE_FILES_DIRECTORY}\\longest_lines.shp").drop(columns=["OBJECTID","FIRST_Mode"])
    longest_lines = pd.DataFrame(longest_lines).drop(columns=["geometry"])

    #Attach the longest possible internal trip to each zone
    merged_data = pd.merge(data, longest_lines, left_on=["Origin"], right_on=["Zone_ID"]).drop(columns=["Zone_ID"])

    #Convert to km
    merged_data["Length"] = merged_data["Length"] / 1000
    
    #Attach the areas for vkt calculations
    merged_data = pd.merge(merged_data, study_area_zones, left_on=["Origin"], right_on=["Zone_ID"]).drop(columns=["Zone_ID"])

    #Convert to km squared
    merged_data["Area"] = merged_data["Area"] /1000000

    #Finding a characteristic radius (approxing the zone to be a circle) to give an indication of a typical internal trip, this allows for the vkt to be estimated
    merged_data["Radius"] = np.sqrt(merged_data["Area"]/pi)
    merged_data["Estimated vkt"] = merged_data["Total Demand"] * merged_data["Radius"]

    #Divide the internal trips to apply different thresholds
    rural_trips = merged_data[merged_data["Urban/Rural"] == "Rural"]
    urban_trips = merged_data[merged_data["Urban/Rural"] == "Urban"]

    internal_OD_pairs = len(merged_data.index)
    internal_trips = merged_data["Total Demand"].sum()
    internal_vkt = merged_data["Estimated vkt"].sum()

    logger.info(f"Internal OD pairs: {internal_OD_pairs}")
    logger.info(f"Internal trips: {internal_trips}")
    logger.info(f"Internal estimated vkt: {internal_vkt}")

    high_short_urban_trips = urban_trips[urban_trips["Length"] < high_urban_active_travel_threshold]
    high_long_urban_trips = urban_trips[urban_trips["Length"] > high_urban_active_travel_threshold]
    low_short_urban_trips = urban_trips[urban_trips["Length"] < low_urban_active_travel_threshold]
    low_long_urban_trips = urban_trips[urban_trips["Length"] > low_urban_active_travel_threshold]

    high_short_rural_trips = rural_trips[rural_trips["Length"] < high_rural_active_travel_threshold]
    high_long_rural_trips = rural_trips[rural_trips["Length"] > high_rural_active_travel_threshold]
    low_short_rural_trips = rural_trips[rural_trips["Length"] < low_rural_active_travel_threshold]
    low_long_rural_trips = rural_trips[rural_trips["Length"] > low_rural_active_travel_threshold]

    high_scenario_short_internal_trips = pd.concat([high_short_rural_trips, high_short_urban_trips]).drop(columns=["Length", "Area", "Radius"])
    low_scenario_short_internal_trips = pd.concat([low_short_rural_trips, low_short_urban_trips]).drop(columns=["Length", "Area", "Radius"])
    high_scenario_long_internal_trips = pd.concat([high_long_rural_trips, high_long_urban_trips]).drop(columns=["Length", "Area", "Radius"])
    low_scenario_long_internal_trips = pd.concat([low_long_rural_trips, low_long_urban_trips]).drop(columns=["Length", "Area", "Radius"])

    high_scenario_short_internal_trips.to_csv(f"{OUTPUT_FOLDER}\\high_scenario_short_internal_trips.csv", index=False)
    low_scenario_short_internal_trips.to_csv(f"{OUTPUT_FOLDER}\\low_scenario_short_internal_trips.csv", index=False)
    high_scenario_long_internal_trips.to_csv(f"{OUTPUT_FOLDER}\\high_scenario_long_internal_trips.csv", index=False)
    low_scenario_long_internal_trips.to_csv(f"{OUTPUT_FOLDER}\\low_scenario_long_internal_trips.csv", index=False)

    logger.info("Output complete")