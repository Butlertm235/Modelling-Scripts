Files in the order to run them:

- model_output_processing.py: contains saturn_output_processor(aadt_factor)
This script takes Saturn Tuba outputs and aggregates the 3 time periods, attaching the centroid coordinates to the OD pairs.
Requires a centroid shape file and the 3 time periods csv outputs.

-model_filter.py: contains filtering_model_outputs(filter_distance: bool, 
                            filter_user_class: bool = True, 
                            filter_zero_trips: bool = True,
                            filter_study_area: bool = True,
                            classify_urban_rural: bool = True,
                            filter_internal_zones: bool = True,
                            port_zones: list() = None,
                            distance_buffer_threshold: int = None)
This script filters the model outputs into a reasonable number to perform further analysis.
Requires study areas shape file (and project depending with urban/rural classification).
Inputs allow the user to select which filtering steps are applied (all set to true by default, except for the distance filter).
It also allows the user to enter a list of port zones for filtering and a distance threshold for the buffer filter.

-api_analysis.py: contains cost_and_trip_analysis(active_travel_threshold: float,
                                    max_OD_pairs_analysed: int,
                                    selected_number_of_long_trips: int,
                                    vkt_or_trip_priortised: str)
This script separates the processed model outputs into long and short trips and produces summary stats and plots to detail
the % of vkt and trips represented by the selected number of OD pairs. Also produces the pairs_to_run and shapes csv files.
Requires the user to use the Google API cost website to find relevant costs to project and add them to the cost_df dataframe,
also requires user to change the step between OD pairs run through the API (default is 25000).

-internal_trip_processor.py: contains internal_processor(high_urban_active_travel_threshold, low_urban_active_travel_threshold, high_rural_active_travel_threshold, low_rural_active_travel_threshold)
This script estimates the vkt within internal zones and separates the trips into short (active travel likely) and long trips.
Requires thresholds for for active travel for combinations of high and low scenario and rural and urban.

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
