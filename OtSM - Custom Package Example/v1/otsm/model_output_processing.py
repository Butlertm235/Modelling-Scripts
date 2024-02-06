"""
Author: Thomas Butler

Contributors:
"""

import numpy as np
import pandas as pd
import geopandas as gpd
from pathlib import Path
import logging
import os

logger = logging.getLogger(__name__)

def saturn_output_processor(aadt_factor):
    """
    Function that takes Saturn TUBA outputs and aggregates AM, IP and PM totals.

    Requires AADT factor as an input
    """
    #INPUTS
    PROGRAM_DIRECTORY = os.getcwd()
    MODEL_OUTPUT_DIRECTORY = f"{PROGRAM_DIRECTORY}\\Model Outputs"
    TIME_PERIODS = ["AM", "IP", "PM"]

    fmt = "{asctime} [{name:40.40}] [{levelname:^10.10}] {message}"
    logging.basicConfig(level=logging.INFO, format=fmt, style="{")
    logger.info("Beginning model output processing")
    logger.info("Loading data")

    #Load raw data into a list
    model_outputs = []
    for time_period in TIME_PERIODS:
        model_outputs.append(pd.read_csv(f"{MODEL_OUTPUT_DIRECTORY}\\{time_period}_Demand.csv", header=None))

    logger.info("Loading centroids")
    #Extract coordinates of all SWRTM zones
    zone_centroids = gpd.read_file(f"{MODEL_OUTPUT_DIRECTORY}\\SWRTM_Centroids.shp")
    zone_coordinates = pd.DataFrame(zone_centroids).drop(columns=['geometry'])
    zone_coordinates['Zone Numbe'] = zone_coordinates['Zone Numbe'].astype('Int64')

    logger.info("All files loaded")

    #Split the raw data that is in TUBA output into separate columns
    for data in model_outputs:
        data['Origin'] = data[0].str.slice(stop=8)
        data['Destination'] = data[0].str.slice(start=8,stop=16)
        data['User Class'] = data[0].str.slice(start=16,stop=24)
        data['Demand'] = data[0].str.slice(start=24).astype(float)
        data['Demand'] = data['Demand'].replace(np.nan, 0)
        data.drop(columns=[0], inplace=True)
    
    #Create a dataframe that has all OD pairs and user class combinations
    aggregated_data = pd.concat(model_outputs).drop(columns=['Demand'])
    aggregated_data.drop_duplicates(inplace=True)

    #Add the AM peak hour demand and scale it to cover AM peak period
    aggregated_data = pd.merge(aggregated_data, model_outputs[0],how='left', left_on=['Origin', 'Destination', 'User Class'], right_on=['Origin', 'Destination', 'User Class']).fillna(0)
    aggregated_data["Demand"] = aggregated_data["Demand"] * 3

    #Add the IP peak hour demand and scale it to cover IP peak period
    aggregated_data = pd.merge(aggregated_data, model_outputs[1],how='left', left_on=['Origin', 'Destination', 'User Class'], right_on=['Origin', 'Destination', 'User Class']).fillna(0)
    aggregated_data["Demand_y"] = aggregated_data['Demand_y'] * 6

    #Add together the demands from AM and IP
    aggregated_data['Total Demand'] = aggregated_data['Demand_x'] + aggregated_data['Demand_y']
    aggregated_data.drop(columns=['Demand_x', 'Demand_y'], inplace=True)

    #Add the PM peak hour demand and scale it to cover PM peak period
    aggregated_data = pd.merge(aggregated_data, model_outputs[2], how='left', left_on=['Origin', 'Destination', 'User Class'], right_on=['Origin', 'Destination', 'User Class']).fillna(0)
    aggregated_data["Demand"] = aggregated_data["Demand"] * 3

    #Add together the total demand and IP demand and then scale using the AADT factor
    aggregated_data['Total Demand'] += aggregated_data['Demand']

    aggregated_data.drop(columns=['Demand'], inplace=True)
    aggregated_data['Total Demand'] = aggregated_data['Total Demand'] * aadt_factor

    #Add origin zone coordinates
    aggregated_data['Origin'] = aggregated_data['Origin'].astype('Int64')
    aggregated_data = pd.merge(aggregated_data, zone_coordinates, how='left', left_on='Origin', right_on='Zone Numbe').drop(['Zone Numbe'], axis=1)
    aggregated_data = aggregated_data.rename(columns={"X": "Origin_x", "Y":"Origin_y"})

    #Add destination zone coordinates
    aggregated_data['Destination'] = aggregated_data['Destination'].astype('Int64')
    aggregated_data = pd.merge(aggregated_data, zone_coordinates, how='left', left_on='Destination', right_on='Zone Numbe').drop(['Zone Numbe'], axis=1)
    aggregated_data = aggregated_data.rename(columns={"X": "Dest_x", "Y":"Dest_y"})

    #Output with geometry format that can be loaded into GIS or Geopandas. The geometry describes straight lines between the OD centroids
    output = aggregated_data
    output[["Origin_x", "Origin_y", "Dest_x", "Dest_y"]] = output[["Origin_x", "Origin_y", "Dest_x", "Dest_y"]].astype(str)
    output["wkt_geometry"] = "linestring(" + output["Origin_x"] + " " + output["Origin_y"] + ", " + output["Dest_x"] + " " + output["Dest_y"] + ")"

    logger.info("Outputting processed model outputs")
    output.to_csv("all_model_pairs.csv", index=False)

