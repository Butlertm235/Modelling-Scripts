"""
A code that combines the shapes all outputs from the Google results and the GIS analysis completed by James Tippins.

Author: Thomas Butler

Contributors:

Last Updated: 09/01/2024
"""

import logging
import pandas as pd
import geopandas as gpd

#INPUTS
GIS_ANALYSIS_SHAPE_FILE = 'Shape Files\\tippins_shapes.shp'
GOOGLE_SHAPES_ALL_FILE = 'long_shapes_all.csv'
ACTIVE_TRAVEL_THRESHOLD = 8
OUTPUT_FILE_NAME = 'shapes_all.csv'


##########################################################################################################################################

#PROCEDURE
#Format logger outputs
logger = logging.getLogger(__name__)
fmt = "{asctime} [{name:40.40}] [{levelname:^10.10}] {message}"
logging.basicConfig(level=logging.INFO, format=fmt, style="{")

#Read GIS analysis shape file
logger.info("Loading GIS analysis.")
data = gpd.read_file(GIS_ANALYSIS_SHAPE_FILE)
data = data[["Origin_Zon", "Destinatio", "Length_km", "geometry"]]

logger.warning("Ensure Google mode info file is not overwritten by output from script!")
shapes = pd.read_csv(GOOGLE_SHAPES_ALL_FILE)

#Select trips under the active travel threshold
data = data[data["Length_km"] <= ACTIVE_TRAVEL_THRESHOLD]
data.rename(columns={"Origin_Zon":"origin","Destinatio":"dest", "Length_km":"walking_kms_fastest", "geometry":"walking_fastest_geometry"}, inplace=True)

#As the active travel distances are the shortest possible via both walking and cycling we can copy the data
data["bicycling_fastest_geometry"] = data["walking_fastest_geometry"].copy()
data["bicylcing_kms_fastest"] = data["walking_kms_fastest"].copy()
data["pair_id"] = data["origin"].astype(str) + "_" + data["dest"].astype(str)
data.drop(columns=["origin", "dest"], inplace=True)

shapes = pd.merge(shapes, data, how='left', left_on="pair_id", right_on="pair_id")

shapes.to_csv(OUTPUT_FILE_NAME)