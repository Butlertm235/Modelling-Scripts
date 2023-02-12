#Hussein Falih
#WSP
#Feb, 2022

#This script reads Excel file which contain X,Y coordinates and creates a travel time matrix using Google Distance Matrix API
#More information regarding the API can be found here: https://developers.google.com/maps/documentation/distance-matrix/overview


import pandas as pd
import googlemaps


#Read in the datafile with the X & Y coordinates. X-coordinate = Latitude, Y-coordinate = Longitude


#Read CSV file into data frame named 'df'
#change seperator (sep e.g. ',') type if necessary

df = pd.read_excel("Coordinates.xlsx")

#Perform request to use the Google Maps API web service
#The user should add a key below. The key is generated using google maps account (https://developers.google.com/maps/documentation/distance-matrix/get-api-key)
gmaps = googlemaps.Client(key="AIzaSyDelw8Pc9orbpgWpVQMcmygBEAML8yIgag")

#empty list to store the travel time and distance values
time_list = []
distance_list = []
origin_id_list = []
destination_id_list = []
origin_name_list = []
destination_name_list = []

#Iterate over the pandas dataframe and feed the coordinates values to the API. 

for (i1, row1) in df.iterrows():
  LatOrigin = row1['latitude']
  LongOrigin = row1['longitude']
  origin = (LatOrigin, LongOrigin)
  origin_id = row1['ID'] 
  origin_name = row1['Location']
  for (i2, row2) in  df.iterrows():
    LatDestination = row2['latitude']
    LongDestination = row2['longitude']
    destination_id = row2['ID']
    destination_name = row2['Location']
    destination = (LatDestination, LongDestination)
	#The API has many features where the uses can adjust. In this example, the departure time is set using Unix time and the mode of transport is 
	#driving and the tolls must be avoided. 
    result = gmaps.distance_matrix(origin, destination, mode = 'driving', departure_time='now')
    result_distance = result["rows"][0]["elements"][0]["distance"]["value"]
    result_time = result["rows"][0]["elements"][0]["duration_in_traffic"]["value"]

    time_list.append(result_time)
    distance_list.append(result_distance)
    origin_id_list.append(origin_id)
    origin_name_list.append(origin_name)
    destination_id_list.append(destination_id)
    destination_name_list.append(destination_name)


output = pd.DataFrame()
output['Origin_Name'] = origin_name_list
output['Destination_Name'] = destination_name_list
output['Distance in Meters'] = distance_list
output['Duration in Seconds'] = time_list
output['Duration in Minutes'] = round(output["Duration in Seconds"].apply(lambda x: x/60),1)

time_pivot = pd.pivot_table(output.drop(columns=["Distance in Meters", "Duration in Seconds"]), index="Origin_Name", columns="Destination_Name")
print (time_pivot)

distance_pivot = pd.pivot_table(output.drop(columns=["Duration in Seconds", "Duration in Minutes"]), index="Origin_Name", columns="Destination_Name")
print (distance_pivot)

time_pivot.to_excel("Travel Times.xlsx")
distance_pivot.to_excel("Travel Distances.xlsx")