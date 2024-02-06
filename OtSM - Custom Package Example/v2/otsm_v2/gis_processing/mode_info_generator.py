"""
A code that combines the mode info outputs from the Google results and the GIS analysis completed by James Tippins.

Author: Thomas Butler

Contributors:

Last Updated: 09/01/2024
"""

import logging
import pandas as pd
import geopandas as gpd

#INPUTS
GIS_ANALYSIS_SHAPE_FILE = 'Shape Files\\tippins_shapes.shp'
GOOGLE_MODE_INFO_FILE = 'long_mode_info.csv'
OUTPUT_FILE_NAME = 'short_mode_info.csv'


##########################################################################################################################################

#PROCEDURE
#Format logger outputs
logger = logging.getLogger(__name__)
fmt = "{asctime} [{name:40.40}] [{levelname:^10.10}] {message}"
logging.basicConfig(level=logging.INFO, format=fmt, style="{")

#Read GIS analysis shape file
logger.info("Loading GIS analysis.")
data = gpd.read_file(GIS_ANALYSIS_SHAPE_FILE)
data = data[["Origin_Zon", "Destinatio", "Total_Dema", "vkt", "Length_km"]]

logger.warning("Ensure Google mode info file is not overwritten by output from script!")
mode_info = pd.read_csv(GOOGLE_MODE_INFO_FILE)

data = data[data["Length_km"] <= 8]
data.rename(columns={"Origin_Zon":"origin","Destinatio":"dest","Total_Dema":"trips", "Length_km":"walking_kms_fastest"}, inplace=True)
data["pair_id"] = data["origin"].astype(str) + "_" + data["dest"].astype(str)

data = pd.merge(data, mode_info, how='left', left_on="pair_id", right_on="pair_id")

data.drop(columns=["origin", "dest", "trips", "vkt"], inplace=True)

#As the active travel distances are the shortest possible via both walking and cycling we can copy the data
#Note: We also only need the distance for the active travel comparison so mins_fastest is not required for walking or cycling
data["bicycling_kms_fastest"] = data["walking_kms_fastest"].copy()

#These columns are still created as to not break the Excel template
data[["walking_options", "walking_mins_fastest", "walking_kms_slowest", "walking_mins_slowest", "bicycling_options", "bicycling_mins_fastest", "bicycling_kms_slowest", "bicycling_mins_slowest"]] = 0

#The column order is changed to make copy and pasting simple
data = data[["pair_id", "transit_options", "transit_kms_fastest", "transit_mins_fastest", "transit_kms_slowest", "transit_mins_slowest", "driving_options", "driving_kms_fastest", "driving_mins_fastest", "driving_kms_slowest", "driving_mins_slowest", "walking_options", "walking_kms_fastest", "walking_mins_fastest", "walking_kms_slowest", "walking_mins_slowest", "bicycling_options", "bicycling_kms_fastest", "bicycling_mins_fastest", "bicycling_kms_slowest", "bicycling_mins_slowest", "PT_tube_km", "PT_tram_km", "PT_bus_km", "PT_walk_km", "PT_ferry_km", "PT_train_km"]]

data.to_csv(OUTPUT_FILE_NAME, index=False)