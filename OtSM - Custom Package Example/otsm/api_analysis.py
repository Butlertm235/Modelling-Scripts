"""
Author: Thomas Butler

Contributors:
"""

from bng_latlon import OSGB36toWGS84
import matplotlib.pyplot as plt
from pathlib import Path
import seaborn as sns
import pandas as pd
import numpy as np
import logging
import os

logger = logging.getLogger(__name__)

def coords_to_lat_lon(row):
    """
    Function that converts BNG coords into lattitude and longitude
    """
    origin_easting = row.Origin_x
    origin_northing = row.Origin_y
    dest_easting = row.Dest_x
    dest_northing = row.Dest_y
    origin_lat, origin_lon = OSGB36toWGS84(origin_easting, origin_northing)
    dest_lat, dest_lon = OSGB36toWGS84(dest_easting, dest_northing)
    row["origin_lat"] = origin_lat
    row["origin_lon"] = origin_lon
    row["dest_lat"] = dest_lat
    row["dest_lon"] = dest_lon
    return row

def produce_output_files(long_trips: 'pd.DataFrame',
                                    active_travel_data: 'pd.DataFrame',
                                    OUTPUTS_DIRECTORY: str,
                                    selected_number_of_long_trips: int,
                                    total_pairs: int,
                                    total_trips: float,
                                    total_vkt: float,
                                    short_pairs: int,
                                    short_trips: float,
                                    short_vkt: float,
                                    internals: 'pd.DataFrame'
                                    ):
    """
    Function that takes the selected trips (sorted by the either vkt or trips) and then produces the pairs_to_run, analysed_trip_stats and shape csv files.

    Note: long trips in this instance have already been either vkt or trip sorted before passing into this function
    """

    long_trips_selected = long_trips.head(selected_number_of_long_trips)

    #Create a unique indentifier for each OD pair
    active_travel_data["pair_id"] = active_travel_data["Origin"].astype(str) + '_' + active_travel_data["Destination"].astype(str)
    long_trips_selected["pair_id"] = long_trips["Origin"].astype(str) + '_' + long_trips["Destination"].astype(str)

    #Convert the coordinates to lat, long for use in the Google API
    logger.info("Converting active travel coordinates")
    active_travel_data = active_travel_data.apply(coords_to_lat_lon, axis=1)
    logger.info("Converting long trip coordinates")
    long_trips_selected = long_trips_selected.apply(coords_to_lat_lon, axis=1)

    #Produce csv files that have summaries
    active_travel_data.to_csv(f"{OUTPUTS_DIRECTORY}\\short_trips.csv", index=False)
    long_trips_selected.drop(columns=["trip_sum", "vkt_sum","ODs_run", "perc_trips", "perc_vkt"]).to_csv(f"{OUTPUTS_DIRECTORY}\\long_trips.csv", index=False)

    #Output pairs_to_run files
    active_travel_data[["pair_id", "origin_lat", "origin_lon", "dest_lat", "dest_lon"]].to_csv(f"{OUTPUTS_DIRECTORY}\\short_pairs_to_run.csv", index = False)
    long_trips_selected[["pair_id", "origin_lat", "origin_lon", "dest_lat", "dest_lon"]].to_csv(f"{OUTPUTS_DIRECTORY}\\long_pairs_to_run.csv", index = False)

    #Convert columns to strings to create geometry column
    active_travel_data[["origin_lat", "origin_lon", "dest_lat", "dest_lon"]] = active_travel_data[["origin_lat", "origin_lon", "dest_lat", "dest_lon"]].astype(str)
    long_trips_selected[["origin_lat", "origin_lon", "dest_lat", "dest_lon"]] = long_trips_selected[["origin_lat", "origin_lon", "dest_lat", "dest_lon"]].astype(str)

    active_travel_data['geometry'] = "linestring(" + active_travel_data["origin_lon"] + " " + active_travel_data["origin_lat"] + ", " + active_travel_data["dest_lon"] + " " + active_travel_data["dest_lat"] + ")"
    long_trips_selected['geometry'] = "linestring(" + long_trips_selected["origin_lon"] + " " + long_trips_selected["origin_lat"] + ", " + long_trips_selected["dest_lon"] + " " + long_trips_selected["dest_lat"] + ")"

    #Output shape csv files
    active_travel_data[["pair_id", "Total Demand", "vkt", "geometry"]].to_csv(f"{OUTPUTS_DIRECTORY}\\short_trip_shapes.csv")
    long_trips_selected[["pair_id", "Total Demand", "vkt", "geometry"]].to_csv(f"{OUTPUTS_DIRECTORY}\\long_trip_shapes.csv")

    pairs_analysed = len(long_trips_selected.index) + len(active_travel_data.index)
    trips_analysed = long_trips_selected["Total Demand"].sum() + short_trips
    vkt_analysed = long_trips_selected["vkt"].sum() + short_vkt

    pairs_not_analysed = total_pairs - pairs_analysed
    trips_not_analysed = total_trips - trips_analysed
    vkt_not_analysed = total_vkt - vkt_analysed

    internal_pairs = len(internals.index)
    internal_trips = internals["Total Demand"].sum()
    internal_vkt = internals["vkt"].sum()

    pair_stats = [internal_pairs, pairs_not_analysed, short_pairs, len(long_trips_selected.index)]
    trip_stats = [internal_trips, trips_not_analysed, short_trips, long_trips_selected["Total Demand"].sum()]
    vkt_stats = [internal_vkt, vkt_not_analysed, short_vkt, long_trips_selected["vkt"].sum()]

    stats = pd.DataFrame({"Category": ["Internal trips", "Not analysed", "Short trips < 8km", "Long trips >8km"] , "OD Pairs": pair_stats, "Trips": trip_stats, "VKT": vkt_stats})
    stats.to_csv(f"{OUTPUTS_DIRECTORY}\\Filter stats\\analysed_trip_stats.csv", index=False)

def cost_and_trip_analysis(active_travel_threshold: float,
                                    max_OD_pairs_analysed: int,
                                    selected_number_of_long_trips: int,
                                    vkt_or_trip_priortised: str):
    """
    Function that produces Google Api cost analysis for select number of trips and then outputs the pairs to run and shape csv files for the selected trips.

    Requires input of the active travel threshold (in km), the maximum OD pairs that may be run through the API,
    the selected number of long trips and if the trips should be selected priortising vkt or trip numbers.

    vkt_or_priortised should either be 'vkt' or 'trip'
    """

    fmt = "{asctime} [{name:40.40}] [{levelname:^10.10}] {message}"
    logging.basicConfig(level=logging.INFO, format=fmt, style="{")

    cwd = Path.cwd()

    path = Path.joinpath(cwd, "Plots")
    Path(path).mkdir(parents=True, exist_ok=True)

    path = Path.joinpath(cwd, "Outputs\\Analysed_Internals")
    Path(path).mkdir(parents=True, exist_ok=True)

    path = Path.joinpath(cwd, "Outputs\\For API Cost Analysis")
    Path(path).mkdir(parents=True, exist_ok=True)

    #Ensures user has input function parameters correctly
    if vkt_or_trip_priortised != "vkt" and vkt_or_trip_priortised != "trip":
        raise Exception("Second function input must either be 'vkt' or 'trip'.")
    
    elif selected_number_of_long_trips < 0:
        raise Exception("Enter a positive integer for the number of selected trips.") 

    PROGRAM_DIRECTORY = os.getcwd()
    PLOT_DIRECTORY = f"{PROGRAM_DIRECTORY}\\Plots"
    OUTPUTS_DIRECTORY = f"{PROGRAM_DIRECTORY}\\Outputs"
    COST_ANALYSIS_DIRECTORY = f"{OUTPUTS_DIRECTORY}\\For API Cost Analysis"

    data = pd.read_csv("processed_model_outputs.csv")
    internals = pd.read_csv(f"{OUTPUTS_DIRECTORY}\\internal_trips.csv")
    perc = '%'

    #Calculate statistics for total data for later calculations
    total_pairs = len(data.index)
    total_trips = data["Total Demand"].sum()
    total_vkt = data["vkt"].sum()

    logger.info(f"Cost and trip analysis beginning with {total_pairs} initial OD pairs")

    #Separate the data into long and short trips depending if they are possible by active travel
    active_travel_data = data[data["length"] < active_travel_threshold]
    long_trips = data[~(data["length"] < active_travel_threshold)]

    active_travel_data.to_csv(f"{OUTPUTS_DIRECTORY}\\short_trips.csv")

    #Calculate data about active travel trips
    short_pairs = len(active_travel_data.index)
    short_trips = active_travel_data["Total Demand"].sum()
    short_trips_perc = (short_trips / total_trips) * 100
    short_vkt = active_travel_data["vkt"].sum()
    short_vkt_perc = (short_vkt / total_vkt) * 100

    logger.info(f"Short OD pairs: {short_pairs}")
    logger.info(f"Short trips: {short_trips}")
    logger.info(f"Short trips as {perc} of possible to analyse: {short_trips_perc}")
    logger.info(f"Short vkt: {short_vkt}")
    logger.info(f"Short vkt as {perc} of possible to analyse: {short_vkt_perc}")

    #Calculate data about all possible long trips
    total_long_pairs = len(long_trips.index)
    total_long_trips = long_trips["Total Demand"].sum()
    total_long_trips_perc = (total_long_trips / total_trips) * 100
    total_long_vkt = long_trips["vkt"].sum()
    total_long_vkt_perc = (total_long_vkt / total_vkt) * 100

    logger.info(f"All long OD pairs: {total_long_pairs}")
    logger.info(f"All long trips: {total_long_trips}")
    logger.info(f"All long trips as {perc} of possible to analyse: {total_long_trips_perc}")
    logger.info(f"All long vkt: {total_long_vkt}")
    logger.info(f"All long vkt as {perc} of possible to analyse: {total_long_vkt_perc}")

    #Make a dataframe that can be used to assess Google API cost
    cost_df = pd.DataFrame(columns=["ODs_run", "Cost"])
    cost_df["ODs_run"] = np.arange(1, max_OD_pairs_analysed + 1)

    #TODO: update specific values with manually found cost that is relevant to project
    cost_df["Cost"].loc[cost_df["ODs_run"] == 1] = 0
    cost_df["Cost"].loc[cost_df["ODs_run"] == 5000] = 40.09
    cost_df["Cost"].loc[cost_df["ODs_run"] == 10000] = 80.18
    cost_df["Cost"].loc[cost_df["ODs_run"] == 20000] = 160.35
    cost_df["Cost"].loc[cost_df["ODs_run"] == 25000] = 201.33
    cost_df["Cost"].loc[cost_df["ODs_run"] == 30000] = 240.53
    cost_df["Cost"].loc[cost_df["ODs_run"] == 40000] = 320.70
    cost_df["Cost"].loc[cost_df["ODs_run"] == 50000] = 400.88
    cost_df["Cost"].loc[cost_df["ODs_run"] == 60000] = 465.02
    cost_df["Cost"].loc[cost_df["ODs_run"] == 70000] = 529.16
    cost_df["Cost"].loc[cost_df["ODs_run"] == 75000] = 563.71
    cost_df["Cost"].loc[cost_df["ODs_run"] == 80000] = 593.30
    cost_df["Cost"].loc[cost_df["ODs_run"] == 90000] = 657.44
    cost_df["Cost"].loc[cost_df["ODs_run"] == 100000] = 721.58
    cost_df["Cost"].loc[cost_df["ODs_run"] == 110000] = 787.84
    cost_df["Cost"].loc[cost_df["ODs_run"] == 120000] = 852.15
    cost_df["Cost"].loc[cost_df["ODs_run"] == 125000] = 885.83
    cost_df["Cost"].loc[cost_df["ODs_run"] == 130000] = 916.46
    cost_df["Cost"].loc[cost_df["ODs_run"] == 140000] = 980.78
    cost_df["Cost"].loc[cost_df["ODs_run"] == 150000] = 1045.09
    cost_df["Cost"].loc[cost_df["ODs_run"] == 160000] = 1109.40
    cost_df["Cost"].loc[cost_df["ODs_run"] == 170000] = 1173.72
    cost_df["Cost"].loc[cost_df["ODs_run"] == 175000] = 1207.95
    cost_df["Cost"].loc[cost_df["ODs_run"] == 180000] = 1238.03
    cost_df["Cost"].loc[cost_df["ODs_run"] == 190000] = 1302.34
    cost_df["Cost"].loc[cost_df["ODs_run"] == 200000] = 1366.66

    if vkt_or_trip_priortised == "vkt":
        #Priortise data by vkt
        vkt_sorted_data = long_trips.sort_values(by="vkt", ascending=False)
        
        #TODO: update value in head function that is relevant to project (smaller project, smaller number of ODs realistically run)
        max_analysed_by_vkt = vkt_sorted_data.head(max_OD_pairs_analysed)

        vkt_sorted_trips = []
        vkt_sorted_vkt = []
        vkt_sorted_trip_perc = []
        vkt_sorted_vkt_perc = []

        #TODO: change step so relevant to project
        for x in range(0, (max_OD_pairs_analysed + 1), 25000):
            sample_of_pairs = vkt_sorted_data.head(x)
            vkt_sorted_trips.append(sample_of_pairs["Total Demand"].sum())
            vkt_sorted_vkt.append(sample_of_pairs["vkt"].sum())
            vkt_sorted_trip_perc.append((sample_of_pairs["Total Demand"].sum()/ total_trips) * 100)
            vkt_sorted_vkt_perc.append((sample_of_pairs["vkt"].sum()/ total_vkt) * 100)

        #Output the cost analysis information for use in presentation to the client or PM
        vkt_sorted_info = pd.DataFrame()
        vkt_sorted_info["OD Pairs"] = range(0, (max_OD_pairs_analysed + 1), 25000)
        vkt_sorted_info.sort_values(by="OD Pairs", ignore_index=True)
        vkt_sorted_info["Trips"] = vkt_sorted_trips
        vkt_sorted_info[f"{perc}Trips"] = vkt_sorted_trip_perc
        vkt_sorted_info["vkt"] = vkt_sorted_vkt
        vkt_sorted_info[f"{perc}vkt"] = vkt_sorted_vkt_perc
        vkt_sorted_info = pd.merge(vkt_sorted_info, cost_df, how='left', left_on=["OD Pairs"], right_on=["ODs_run"])
        logger.info("Outputting API cost statistics for vkt ordered data")
        vkt_sorted_info.to_csv(f"{COST_ANALYSIS_DIRECTORY}\\vkt_sorted_info.csv", index=False)

        #Calculate demand and vkt sum across all long OD pairs to use in plots
        vkt_sorted_data["trip_sum"] = vkt_sorted_data["Total Demand"].cumsum()
        vkt_sorted_data["vkt_sum"] = vkt_sorted_data["vkt"].cumsum()
        vkt_sorted_data["ODs_run"] = np.arange(1, len(vkt_sorted_data)+1)
        vkt_sorted_data["perc_trips"] = (vkt_sorted_data["trip_sum"] / total_trips) * 100
        vkt_sorted_data["perc_vkt"] = (vkt_sorted_data["vkt_sum"] / total_vkt) * 100

        #Create a plot to show % of trips covered against OD pairs run
        plt.figure()
        sns.set_style('darkgrid')
        sns.lineplot(x='ODs_run', y='perc_trips', data=vkt_sorted_data)
        plt.ylim(0, 100)
        plt.title(f"{perc} of trips vs OD pairs run - Priortising vkt")
        plt.savefig(f"{PLOT_DIRECTORY}\\vkt_ordered_trip_analysis.png")

        #Create a plot to show % of vkt covered against OD pairs run
        plt.figure()
        sns.lineplot(x='ODs_run', y='perc_vkt', data=vkt_sorted_data)
        plt.ylim(0, 100)
        plt.title(f"{perc} of vkt vs OD pairs run - Priortising vkt")
        plt.savefig(f"{PLOT_DIRECTORY}\\vkt_ordered_vkt_analysis.png")

        #Calculate demand and vkt sum across analysable OD pairs to use in plots
        max_analysed_by_vkt["trip_sum"] = max_analysed_by_vkt["Total Demand"].cumsum()
        max_analysed_by_vkt["vkt_sum"] = max_analysed_by_vkt["vkt"].cumsum()
        max_analysed_by_vkt["ODs_run"] = np.arange(1, len(max_analysed_by_vkt)+1)
        max_analysed_by_vkt["perc_trips"] = (max_analysed_by_vkt["trip_sum"] / total_trips) * 100
        max_analysed_by_vkt["perc_vkt"] = (max_analysed_by_vkt["vkt_sum"] / total_vkt) * 100

        #Create subplots to contain both cost and trip analysis
        fig, ax1 = plt.subplots()

        color = 'tab:blue'
        ax1.set_xlabel('O-D Pairs')
        ax1.set_ylabel(f'{perc} of trips', color=color)
        ax1.set_ylim(0, 100)
        ax1.plot(max_analysed_by_vkt["ODs_run"], max_analysed_by_vkt['perc_trips'], color=color)
        ax1.tick_params(axis='y', labelcolor=color)

        #Copy x axis to ensure cost analysis is at the same scale as the trip analysis
        ax2 = ax1.twinx()

        color = 'tab:red'
        ax2.set_ylabel('Cost (£)', color=color) 
        ax2.set_ylim(0, 1500)
        ax2.scatter(max_analysed_by_vkt["ODs_run"], cost_df['Cost'], color=color)
        ax2.tick_params(axis='y', labelcolor=color)

        fig.tight_layout() 
        plt.title(f"{perc} of trips / Cost of API run vs OD pairs run - Priortising vkt (Top {max_OD_pairs_analysed})")
        plt.savefig(f"{PLOT_DIRECTORY}\\vkt_ordered_trip_analysis_{max_OD_pairs_analysed}.png")

        #Create subplots to contain both cost and vkt analysis
        fig, ax1 = plt.subplots()

        color = 'tab:blue'
        ax1.set_xlabel('O-D Pairs')
        ax1.set_ylabel(f'{perc} of vkt', color=color)
        ax1.set_ylim(0, 100)
        ax1.plot(max_analysed_by_vkt["ODs_run"], max_analysed_by_vkt['perc_vkt'], color=color)
        ax1.tick_params(axis='y', labelcolor=color)

        #Copy x axis to ensure cost analysis is at the same scale as the vkt analysis
        ax2 = ax1.twinx()

        color = 'tab:red'
        ax2.set_ylabel('Cost (£)', color=color)
        ax2.set_ylim(0, 1500)
        ax2.scatter(max_analysed_by_vkt["ODs_run"], cost_df['Cost'], color=color)
        ax2.tick_params(axis='y', labelcolor=color)

        fig.tight_layout()
        plt.title(f"{perc} of vkt / Cost of API run vs OD pairs run - Priortising vkt (Top {max_OD_pairs_analysed})")
        plt.savefig(f"{PLOT_DIRECTORY}\\vkt_ordered_vkt_analysis_{max_OD_pairs_analysed}.png")
        plt.show()

        produce_output_files(vkt_sorted_data, active_travel_data, OUTPUTS_DIRECTORY, selected_number_of_long_trips, total_pairs, total_trips, total_vkt, short_pairs, short_trips, short_vkt, internals)

    elif vkt_or_trip_priortised == "trip":
        #Priortise data by trips
        trip_sorted_data = long_trips.sort_values(by="Total Demand", ascending=False)
        
        #TODO: update value in head function that is relevant to project (smaller project, smaller number of ODs realistically run)
        max_analysed_by_trip = trip_sorted_data.head(max_OD_pairs_analysed)

        trip_sorted_trips = []
        trip_sorted_vkt = []
        trip_sorted_trip_perc = []
        trip_sorted_vkt_perc = []

        #TODO: change step so relevant to project
        for x in range(0, (max_OD_pairs_analysed + 1), 25000):
            sample_of_pairs = trip_sorted_data.head(x)
            trip_sorted_trips.append(sample_of_pairs["Total Demand"].sum())
            trip_sorted_vkt.append(sample_of_pairs["vkt"].sum())
            trip_sorted_trip_perc.append((sample_of_pairs["Total Demand"].sum()/ total_trips) * 100)
            trip_sorted_vkt_perc.append((sample_of_pairs["vkt"].sum()/ total_vkt) * 100)

        #Output the cost analysis information for use in presentation to the client or PM
        trip_sorted_info = pd.DataFrame()
        trip_sorted_info.sort_values(by="OD Paris", ignore_index=True)
        trip_sorted_info["OD Pairs"] = range(0, (max_OD_pairs_analysed + 1), 25000)
        trip_sorted_info["Trips"] = trip_sorted_trips
        trip_sorted_info[f"{perc}Trips"] = trip_sorted_trip_perc
        trip_sorted_info["vkt"] = trip_sorted_vkt
        trip_sorted_info[f"{perc}vkt"] = trip_sorted_vkt_perc
        trip_sorted_info = pd.merge(trip_sorted_info, cost_df, how='left', left_on=["OD Pairs"], right_on=["ODs_run"])
        logger.info("Outputting API cost statistics for trip sorted data")
        trip_sorted_info.to_csv(f"{COST_ANALYSIS_DIRECTORY}\\trip_sorted_info.csv", index=False)

        #Calculate demand and vkt sum across all long OD pairs to use in plots
        trip_sorted_data["trip_sum"] = trip_sorted_data["Total Demand"].cumsum()
        trip_sorted_data["vkt_sum"] = trip_sorted_data["vkt"].cumsum()
        trip_sorted_data["ODs_run"] = np.arange(1, len(trip_sorted_data)+1)
        trip_sorted_data["perc_trips"] = (trip_sorted_data["trip_sum"] / total_trips) * 100
        trip_sorted_data["perc_vkt"] = (trip_sorted_data["vkt_sum"] / total_vkt) * 100

        #Create a plot to show % of trips covered against OD pairs run
        plt.figure()
        sns.set_style('darkgrid')
        sns.lineplot(x='ODs_run', y='perc_trips', data=trip_sorted_data)
        plt.ylim(0, 100)
        plt.title(f"{perc} of trips vs OD pairs run - Priortising trips")
        plt.savefig(f"Plots\\trip_ordered_trip_analysis.png")

        #Create a plot to show % of vkt covered against OD pairs run
        plt.figure()
        sns.lineplot(x='ODs_run', y='perc_vkt', data=trip_sorted_data)
        plt.ylim(0, 100)
        plt.title(f"{perc} of vkt vs OD pairs run - Priortising trips")
        plt.savefig(f"Plots\\trip_ordered_vkt_analysis.png")

        #Calculate demand and vkt sum across analysable OD pairs to use in plots
        max_analysed_by_trip["trip_sum"] = max_analysed_by_trip["Total Demand"].cumsum()
        max_analysed_by_trip["vkt_sum"] = max_analysed_by_trip["vkt"].cumsum()
        max_analysed_by_trip["ODs_run"] = np.arange(1, len(max_analysed_by_trip)+1)
        max_analysed_by_trip["perc_trips"] = (max_analysed_by_trip["trip_sum"] / total_trips) * 100
        max_analysed_by_trip["perc_vkt"] = (max_analysed_by_trip["vkt_sum"] / total_vkt) * 100

        #Create subplots to contain both cost and trip analysis
        fig, ax1 = plt.subplots()

        color = 'tab:blue'
        ax1.set_xlabel('O-D Pairs')
        ax1.set_ylabel(f'{perc} of trips', color=color)
        ax1.set_ylim(0, 100)
        ax1.plot(max_analysed_by_trip["ODs_run"], max_analysed_by_trip['perc_trips'], color=color)
        ax1.tick_params(axis='y', labelcolor=color)

        #Copy x axis to ensure cost analysis is at the same scale as the trip analysis
        ax2 = ax1.twinx()

        color = 'tab:red'
        ax2.set_ylabel('Cost (£)', color=color) 
        ax2.set_ylim(0, 1500)
        ax2.scatter(max_analysed_by_trip["ODs_run"], cost_df['Cost'], color=color)
        ax2.tick_params(axis='y', labelcolor=color)

        fig.tight_layout() 
        plt.title(f"{perc} of trips / Cost of API run vs OD pairs run - Priortising trips (Top {max_OD_pairs_analysed})")
        plt.savefig(f"{PLOT_DIRECTORY}\\trip_ordered_trip_analysis_{max_OD_pairs_analysed}.png")
        plt.show()

        #Create subplots to contain both cost and vkt analysis
        fig, ax1 = plt.subplots()

        color = 'tab:blue'
        ax1.set_xlabel('O-D Pairs')
        ax1.set_ylabel(f'{perc} of vkt', color=color)
        ax1.set_ylim(0, 100)
        ax1.plot(max_analysed_by_trip["ODs_run"], max_analysed_by_trip['perc_vkt'], color=color)
        ax1.tick_params(axis='y', labelcolor=color)

        #Copy x axis to ensure cost analysis is at the same scale as the vkt analysis
        ax2 = ax1.twinx()

        color = 'tab:red'
        ax2.set_ylabel('Cost (£)', color=color)
        ax2.set_ylim(0, 1500)
        ax2.scatter(max_analysed_by_trip["ODs_run"], cost_df['Cost'], color=color)
        ax2.tick_params(axis='y', labelcolor=color)

        fig.tight_layout()
        plt.title(f"{perc} of vkt / Cost of API run vs OD pairs run - Priortising trips (Top {max_OD_pairs_analysed})")
        plt.savefig(f"{PLOT_DIRECTORY}\\trip_ordered_vkt_analysis_{max_OD_pairs_analysed}.png")
        plt.show()

        produce_output_files(trip_sorted_data, active_travel_data, OUTPUTS_DIRECTORY, selected_number_of_long_trips, total_pairs, total_trips, total_vkt, short_pairs, short_trips, short_vkt, internals)
        
    else:
        print("Trip or vkt priortisation not chosen correctly")
