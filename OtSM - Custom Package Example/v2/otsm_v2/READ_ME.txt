Files in the order to run them:

- model_output_processing.py: contains saturn_output_processor(aadt_factor)
This script takes Saturn TUBA 3 outputs and aggregates the 3 time periods, attaching the centroid coordinates to the OD pairs.
Requires a centroid shape file and the 3 time periods csv outputs.
Required the input of the centroid shape file name/directory and the Saturn outputs should have a name in the format: "[time period]_Demand.csv"

-model_filter.py contains:  filtering_model_outputs(filter_distance: bool = False,
                                                    distance_buffer_threshold: int = None,
                                                    filter_short_trips_for_GIS: bool = False,
                                                    filter_trips_below_1_pcu: bool = False, 
                                                    filter_user_class: bool = True, 
                                                    filter_zero_trips: bool = True,
                                                    filter_origin_study_area: bool = True,
                                                    filter_origin_destination_study_area: bool = False,
                                                    classify_urban_rural: bool = True,
                                                    filter_internal_zones: bool = True,
                                                    port_zones: list() = None)
This script filters the model outputs into a reasonable number to perform further analysis.
Requires study areas shape file (and project depending with urban/rural classification).
Inputs allow the user to select which filtering steps are applied (all set to true by default, except for the distance filter, study area 
by origin and destination and the 1 pcu filter).
The study area filter can now either filter for OD pairs only originating in the study area and or either originating or ending in the study area. 
It also allows the user to enter a list of port zones for filtering and a distance threshold for the buffer filter.
IMPORTANT UPDATE: Now allows for filter_short_trips_for_GIS to be set to true which allows the user to analyse short trips without the use
of Google API. If selected the scripts in the GIS Processing folder will need to be run which you can find at the bottom of this page.

-api_analysis.py contains: cost_and_trip_analysis(active_travel_threshold: float,
                                    max_OD_pairs_analysed: int,
                                    selected_number_of_long_trips: int,
                                    vkt_or_trip_priortised: str,
                                    generate_api_cost = False):
This script separates the processed model outputs into long and short trips and produces summary stats and plots to detail
the % of vkt and trips represented by the selected number of OD pairs. Also produces the pairs_to_run and straight line shapes csv files.
Script also generates cost of API run based off of the theoretical maximum number of OD pairs for the project. This uses the 
Google API formula so cost estimates are output in dollars.
Requires the input of the active travel threshold in km, the maximum number of OD pairs that are considered for analysis (an upper bound 
used to select how many OD pairs to display on plots),the number of long pairs selected for the API analysis (on the first run just use an 
estimate or the same value as max_OD_pairs_analysed), whether the OD pairs are to be sorted priortising trips (good for behavioural analysis)
or vkt (good for emissions analysis) and finally whether the api cost dataframe should be used.
Note: Maximum number of queries Google API will give outside of contacting sales is 500,000 so max_OD_pairs_analysed can only be 250,000

-internal_trip_processor.py contains: internal_processor(high_urban_active_travel_threshold, 
							                             low_urban_active_travel_threshold, 
							                             high_rural_active_travel_threshold, 
							                             low_rural_active_travel_threshold)
This script estimates the vkt within internal zones and separates the trips into short (active travel likely) and long trips.
Requires thresholds for for active travel for combinations of high and low scenario and rural and urban.

GIS PROCESSING SCRIPTS:
-gis_results_processor.py contains: process_gis_data(high_scenario_active_travel_threshold: float, 
                                                     low_scenario_active_travel_threshold: float)
AFTER the GIS analysis has been complete, this script produces the files required as input into the analysis sheets and prepares files ready 
for an api cost analysis. The files output by this script are 'short_active_shapes_all.csv', 'short_model_outputs.csv', 'long_model_outputs.csv' 
and 'short_trips_for_API_analysis.csv'.
It requires the input of both low and high scenario active travel thresholds in km.

-gis_and_api_analysis.py contains: cost_and_trip_analysis(max_OD_pairs_analysed: int,
                                                          selected_number_of_long_pairs: int,
                                                          vkt_or_trip_priortised: str,
                                                          generate_api_cost = True)    
This script takes the outputs from gis_results_processor and produces the files ready to run through the API as well as the 'pairs to run' inputs
for the analysis sheets. On top of this, the script produce the statistics that can then be used to generate the doughnut diagram in the report.
This includes accounting for the short trips that still need long API analysis in the low scenario. The script also generates plots representing
the cost of the API analysis alongside what % of the trips and vkt are being covered by the cost. The cost is estimated in dollars.
Requires the input of the maximum number of OD pairs that are considered for analysis (an upper bound), the number of long pairs selected for the API
analysis (on the first run just use an estimate or the same value as max_OD_pairs_analysed), whether the OD pairs are to be sorted priortising trips
(good for behavioural analysis) or vkt (good for emissions analysis) and finally whether the api cost dataframe should be used.

EXTERNAL PACKAGES USED:
geopandas
pandas
numpy
logging
shapely
seaborn
bng_latlon
math
os
