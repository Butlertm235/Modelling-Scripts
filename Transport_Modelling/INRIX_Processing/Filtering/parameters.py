# ***EXAMPLE FILE TO STORE IN GITHUB REPOSITORY. REPLACE THIS LINE WITH PROJECT INFO ONCE COPIED INTO PROJECT WORKSPACE***

# parameters file for the distanceFilter function
# edit and save a copy into the same directory as the script used to run the distanceFilter function

import File_scripts.get_filepath as FS
fp = FS.get_filepath_this_script(__file__)

#user input

#directories/files - absolute paths; or relative to the directory of this file using 'fp+' as defined above. Or define a separate directory above.
directory = fp+'Raw Data H' #directory containing only GPS data csv files
outputDirectory = fp+'DistanceFilterFiles_TEST' #directory to contain outputs (may or may not already exist)
gisAttributesFile = fp+'OSHighways220505Congestion_combined.csv' #csv file containing a minimum of link ids and link lengths
selectedLinkIDs = fp+'Input_PR/link_id.csv' #route links file: remove all links not in this file if trimToSelectedLinks = 1 below. Can comment out if not used.

#columns names
gpsLinkId = 'link_id' #column header for link id in the GPS data
gpsDist = 'avg_dist_m' #column header for recorded average distance in GPS data
gisLinkId = 'link_id' #column header for link id in the GIS data (link ids corresponding to GPS data)
gisDist = 'length' #column header for link length in the GIS data
incompleteFlag = 'incomplete_flag'
selectedColumnsUnfiltered = ['link_id','n','Dist_diff','Perc_Dist_diff',incompleteFlag] #selected columns to include in output of unfiltered combined GPS and GIS data
TMJ_columns = ['link_id','network','date_1','time_per','veh_cls','data_source','n','avg_jt','sum_squ_jt'] #columns TMJ tool needs
selectedColumnsFiltered = ['link_id','network','date_1','time_per','veh_cls','data_source','n','avg_jt','sum_squ_jt','avg_speed_kph',gpsDist,gisDist,'Dist_diff','Perc_Dist_diff',incompleteFlag,'roadClassi','routeHiera','formOfWay','roadName1_'] #selected columns to include in output of filtered combined GPS and GIS data


#user filter options (1 for Yes, 0 for no)
incompleteJourneyFilter = 1 #filter out records with an incomplete flag != 0 
distFilterAbs = 0 # filter out differences between GPS and GIS distances by absolute differences
distFilterRel = 1 # filter out differences between GPS and GIS distances by relative (%) differences
distFilterAbsValue = 10 #difference (units of provided data) to exclude data where difference is above or below this value
distFilterRelValue = 1.0 #percentage difference to exclude data where percentage difference is above or below this value (enter 10.0 for 10% etc)
trimToSelectedLinks = 1 #only carry out processes for selected links define in selectedLinkIDs variable above. Remove records for other links.


#output options (1 for Yes, 0 for no)
outputGPSCombined = 1 #return a csv file combining all input GPS data (potential to be very large)
outputGPSCombinedConcatCol = 0 #create amd return a csv file as above plus adding a concatenated link_id,distance column to be kept for rest of script
outputAllCombined = 1 #return a csv file of a merged GPS and GIS dataset
outputAllRouteLinks = 1 #return a route links file with every link of the raw GPS data
outputGISRouteLinks = 1 #return a route links file with every link within the GIS data
outputCombinedRouteLinks = 1 #return a route links file with every link of the raw data also contained within the GIS data
outputdistanceFilteredData = 1 #return a csv file of the distance filtered merged GPS and GIS dataset
outputDistanceFilteredRouteLinks = 1 #return a route links file of links where the difference between GPS and GIS distances is satisfactorily small (based on filter values)
outputFlagFilteredRouteLinks = 1 #return a route links file of links filtered out by the incomplete flag marker
outputRouteLinksToGIS = 1 #merge each respective route links file with the GIS data and output as csv - useful for plotting if GIS data has link geometry (eg WKT)