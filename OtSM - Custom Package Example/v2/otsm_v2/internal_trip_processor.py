"""
Author: Thomas Butler

Contributors:

Last Updated: 12/09/2023
"""

import numpy as np
import pandas as pd
import geopandas as gpd
import logging
import math
import os

logger = logging.getLogger(__name__)

#INPUTS
PROGRAM_DIRECTORY = os.getcwd()
SHAPE_FILES_DIRECTORY = f"{PROGRAM_DIRECTORY}\\Shape Files"
INTERNAL_TRIPS_DIRECTORY = f"{PROGRAM_DIRECTORY}\\Outputs"
OUTPUT_FOLDER = f"{INTERNAL_TRIPS_DIRECTORY}\\Analysed_Internals\\"
ZONE_SHAPE_FILE_NAME = 'HCC Zones.shp'


def internal_processor(high_urban_active_travel_threshold: float, 
                       low_urban_active_travel_threshold: float,
                       longest_line_analysis: bool = True,
                       high_rural_active_travel_threshold: float = 0, 
                       low_rural_active_travel_threshold: float = 0):
    """
    Function that finds the estimated internal vkt and divides the trips into short (possible by active travel) and long.
    If the project has no urban rural split just enter values for the urban active thresholds

    NEW FEATURE: Can opt to use diameter instead of longest line analysis.

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

    study_area_zones = gpd.read_file(f"{SHAPE_FILES_DIRECTORY}\\{ZONE_SHAPE_FILE_NAME}")
    
    study_area_zones = study_area_zones[["Zone_ID", "Zone_area"]]

    if longest_line_analysis:
        
        #Longest lines file should be created using longest lines code which requires QGIS simplification of the zones
        longest_lines = gpd.read_file(f"{SHAPE_FILES_DIRECTORY}\\longest_lines.shp").drop(columns=["OBJECTID","FIRST_Mode", "Zone_area"])
        longest_lines = pd.DataFrame(longest_lines).drop(columns=["geometry"])

        #Attach the longest possible internal trip to each zone
        merged_data = pd.merge(data, longest_lines, left_on=["Origin"], right_on=["Zone_ID"]).drop(columns=["Zone_ID"])

        #Convert to km
        merged_data["Length"] = merged_data["Length"] / 1000
        
        #Attach the areas for vkt calculations
        merged_data = pd.merge(merged_data, study_area_zones, left_on=["Origin"], right_on=["Zone_ID"]).drop(columns=["Zone_ID"])

        #Convert to km squared
        merged_data["Zone_area"] = merged_data["Zone_area"] /1000000

        #Finding a characteristic radius (approxing the zone to be a circle) to give an indication of a typical internal trip, this allows for the vkt to be estimated
        merged_data["Radius"] = np.sqrt(merged_data["Zone_area"]/pi)
        merged_data["Estimated vkt"] = merged_data["Total Demand"] * merged_data["Radius"]

        internal_OD_pairs = len(merged_data.index)
        internal_trips = merged_data["Total Demand"].sum()
        internal_vkt = merged_data["Estimated vkt"].sum()

        logger.info(f"Internal OD pairs: {internal_OD_pairs}")
        logger.info(f"Internal trips: {internal_trips}")
        logger.info(f"Internal estimated vkt: {internal_vkt}")

        if high_rural_active_travel_threshold > 0 and low_rural_active_travel_threshold > 0:

            #Divide the internal trips to apply different thresholds
            rural_trips = merged_data[merged_data["Urban/Rural"] == "Rural"]
            urban_trips = merged_data[merged_data["Urban/Rural"] == "Urban"]

            high_short_urban_trips = urban_trips[urban_trips["Length"] <= high_urban_active_travel_threshold]
            high_long_urban_trips = urban_trips[urban_trips["Length"] > high_urban_active_travel_threshold]
            low_short_urban_trips = urban_trips[urban_trips["Length"] <= low_urban_active_travel_threshold]
            low_long_urban_trips = urban_trips[urban_trips["Length"] > low_urban_active_travel_threshold]

            high_short_rural_trips = rural_trips[rural_trips["Length"] <= high_rural_active_travel_threshold]
            high_long_rural_trips = rural_trips[rural_trips["Length"] > high_rural_active_travel_threshold]
            low_short_rural_trips = rural_trips[rural_trips["Length"] <= low_rural_active_travel_threshold]
            low_long_rural_trips = rural_trips[rural_trips["Length"] > low_rural_active_travel_threshold]

            high_short_trips = pd.concat([high_short_rural_trips, high_short_urban_trips])
            low_short_trips = pd.concat([low_short_rural_trips, low_short_urban_trips])
            high_long_trips = pd.concat([high_long_rural_trips, high_long_urban_trips])
            low_long_trips = pd.concat([low_long_rural_trips, low_long_urban_trips])

            high_short_trips = high_short_trips[["Origin", "Trips", "Estimated vkt", "Urban/Rural"]]
            low_short_trips = low_short_trips[["Origin", "Trips", "Estimated vkt", "Urban/Rural"]]
            high_long_trips = high_long_trips[["Origin", "Trips", "Estimated vkt", "Urban/Rural"]]
            low_long_trips = low_long_trips[["Origin", "Trips", "Estimated vkt", "Urban/Rural"]]

            high_short_trips.to_csv(f"{OUTPUT_FOLDER}\\high_scenario_short_internal_trips.csv", index=False)
            low_short_trips.to_csv(f"{OUTPUT_FOLDER}\\low_scenario_short_internal_trips.csv", index=False)
            high_long_trips.to_csv(f"{OUTPUT_FOLDER}\\high_scenario_long_internal_trips.csv", index=False)
            low_long_trips.to_csv(f"{OUTPUT_FOLDER}\\low_scenario_long_internal_trips.csv", index=False)

            high_trips = pd.concat([high_short_trips, high_long_trips]).sort_values("Origin")
            low_trips = pd.concat([low_short_trips, low_long_trips]).sort_values("Origin")

            high_trips.to_csv(f"{OUTPUT_FOLDER}\\high_scenario_internal_trips.csv", index=False)
            low_trips.to_csv(f"{OUTPUT_FOLDER}\\low_scenario_internal_trips.csv", index=False)
        
        else:

            high_short_trips = merged_data[merged_data["Length"] <= high_urban_active_travel_threshold]
            high_long_trips = merged_data[merged_data["Length"] > high_urban_active_travel_threshold]
            low_short_trips = merged_data[merged_data["Length"] <= low_urban_active_travel_threshold]
            low_long_trips = merged_data[merged_data["Length"] > low_urban_active_travel_threshold]

            high_short_trips.to_csv(f"{OUTPUT_FOLDER}\\high_scenario_short_internal_trips.csv", index=False)
            high_long_trips.to_csv(f"{OUTPUT_FOLDER}\\high_scenario_long_internal_trips.csv", index=False)
            low_short_trips.to_csv(f"{OUTPUT_FOLDER}\\low_scenario_short_internal_trips.csv", index=False)
            low_long_trips.to_csv(f"{OUTPUT_FOLDER}\\low_scenario_long_internal_trips.csv", index=False)

            high_trips = pd.concat([high_short_trips, high_long_trips]).sort_values("Origin")
            low_trips = pd.concat([low_short_trips, low_long_trips]).sort_values("Origin")

            high_trips.to_csv(f"{OUTPUT_FOLDER}\\high_scenario_internal_trips.csv", index=False)
            low_trips.to_csv(f"{OUTPUT_FOLDER}\\low_scenario_internal_trips.csv", index=False)
    
    else:
        #Attach the areas for vkt and diameter calculations
        data = pd.merge(data, study_area_zones, left_on=["Origin"], right_on=["Zone_No"]).drop(columns=["Zone_No"])

        data["Zone_area"] = data["Zone_area"] / 1000000

        #Finding a characteristic radius (approxing the zone to be a circle) to give an indication of a typical internal trip, this allows for the vkt to be estimated
        data["Radius"] = np.sqrt(data["Zone_area"]/pi)
        data["Estimated vkt"] = data["Total Demand"] * data["Radius"]

        #If no longest lines analysis occurs use the characteristic diameter as a length estimate
        data["Diameter"] = data["Radius"] * 2

        data["Estimated vkt"] = data["Total Demand"] * data["Radius"]

        internal_OD_pairs = len(data.index)
        internal_trips = data["Total Demand"].sum()
        internal_vkt = data["Estimated vkt"].sum()

        logger.info(f"Internal OD pairs: {internal_OD_pairs}")
        logger.info(f"Internal trips: {internal_trips}")
        logger.info(f"Internal estimated vkt: {internal_vkt}")


    logger.info("Output complete")