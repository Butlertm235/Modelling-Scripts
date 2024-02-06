"""
Author: Thomas Butler

Contributors:

Last Updated: 26/09/2023
"""

import geopandas as gpd
import pandas as pd
import logging
import os

logger = logging.getLogger(__name__)

#INPUTS
INPUT_LONG_FILE_NAME = 'processed_long_model_outputs.csv'
INPUT_SHORT_FILE_NAME = 'short_trips_for_GIS.csv'
SHORT_GIS_PROCESSED_SHAPE_FILE = 'tippins_shapes.shp'

PROGRAM_DIRECTORY = os.getcwd()
OUTPUTS_DIRECTORY = f"{PROGRAM_DIRECTORY}\\Outputs"

def create_short_active_shapes_all(short_shapes, high_scenario_active_travel_threshold):
    """
    Creates file that contains the shape information for the active travel trips. Should be combined with the relevant long shape information once the Google API has been run.
    """
    logger.info("Creating shapes_all file for active travel routes.")
    short_shapes_all = short_shapes[short_shapes["Length_km"] < high_scenario_active_travel_threshold]
    short_shapes_all = short_shapes_all[["pair_id", "geometry"]]
    short_shapes_all["walking_fastest_geometry"] = short_shapes_all["geometry"].copy()
    short_shapes_all.rename(columns={"geometry":"bicycling_fastest_geometry"})
    short_shapes_all.to_csv("short_active_shapes_all.csv", index=False)

def produce_model_output_files(short_data, long_data, short_shapes, high_scenario_active_travel_threshold):
    """
    Creates the files required for the 'model outputs' tab in the analysis sheet and for the api analysis.
    """

    logger.info("Creating model_output files.")
    real_short_shapes_filter = short_shapes["Length_km"] < high_scenario_active_travel_threshold
    real_short_trips = short_shapes[real_short_shapes_filter]
    real_short_trips = set(real_short_trips["pair_id"])
    
    short_data_filter = short_data.apply(lambda x: x.pair_id in real_short_trips, axis=1)

    short_model_outputs = short_data[short_data_filter]
    long_model_outputs = pd.concat([long_data, short_data[~short_data_filter]])

    short_model_outputs.to_csv(f"{OUTPUTS_DIRECTORY}\\short_model_outputs.csv", index=False)
    long_model_outputs.to_csv(f"{OUTPUTS_DIRECTORY}\\long_model_outputs.csv", index=False)

def produce_short_api_file(short_shapes, short_data, high_scenario_active_travel_threshold, low_scenario_active_travel_threshold):
    """
    Produces file that contains the short trips that do not fall into the active travel range in the low scenario.
    """

    logger.info("Outputting short trips that need PT analysis in the low scenario.")
    api_filter = (short_shapes["Length_km"] > low_scenario_active_travel_threshold) & (short_shapes["Length_km"] < high_scenario_active_travel_threshold)
    short_shapes_for_api = short_shapes[api_filter]
    short_pairs_for_api = set(short_shapes_for_api["pair_id"])
    short_trips_for_api = short_data[short_data.apply(lambda x: x.pair_id in short_pairs_for_api, axis=1)]
    short_trips_for_api.to_csv("short_trips_for_API_analysis.csv", index = False)

def process_gis_data(high_scenario_active_travel_threshold: float = 8, 
                     low_scenario_active_travel_threshold: float = 4.8):
    """
    Produces files required as inputs into the analysis sheet and prepares files ready for an api analysis.

    Requires the input of the active travel threshold in the high and low scenario
    """
    fmt = "{asctime} [{name:40.40}] [{levelname:^10.10}] {message}"
    logging.basicConfig(level=logging.INFO, format=fmt, style="{")

    logger.info("Loading shape file.")
    short_shapes = gpd.read_file(f"Shape Files\\{SHORT_GIS_PROCESSED_SHAPE_FILE}")
    short_shapes["pair_id"] = short_shapes["Origin_Zon"].astype(str) + "_" + short_shapes["Destinatio"].astype(str)
    logger.info("Outputting shape file in csv form.")
    short_shapes.to_csv(f"gis_analysed_shapes.csv", index=False)

    logger.info("Loading long and short model outputs.")
    long_data = pd.read_csv(INPUT_LONG_FILE_NAME)
    long_data["pair_id"] = long_data["Origin"].astype(str) + "_" + long_data["Destination"].astype(str)

    short_data = pd.read_csv(INPUT_SHORT_FILE_NAME)
    short_data["pair_id"] = short_data["Origin"].astype(str) + "_" + short_data["Destination"].astype(str)

    produce_short_api_file(short_shapes, short_data, high_scenario_active_travel_threshold, low_scenario_active_travel_threshold)
    create_short_active_shapes_all(short_shapes, high_scenario_active_travel_threshold)
    produce_model_output_files(short_data, long_data, short_shapes, high_scenario_active_travel_threshold)
    logger.info("Analysis complete!")