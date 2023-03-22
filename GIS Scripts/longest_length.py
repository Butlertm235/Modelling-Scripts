"""
A program that finds the longest straight line that can fit within a zone going from vertex to vertex.
Important Notes: -For this script to run within a reasonable time it is recommended that 

Author: Thomas Butler

Contributors:
"""

import geopandas as gpd
from shapely.geometry import LineString, mapping
from itertools import combinations
import matplotlib.pyplot as plt
from shapely.wkt import loads
import re
import os

#INPUTS
PROGRAM_DIRECTORY = os.getcwd()
FILE_DIRECTORY = f"{PROGRAM_DIRECTORY}"

INPUT_FILE_NAME = "Simplified_Study_Area_Zones.shp"
OUTPUT_FILE_NAME = 'longest_lines.shp'

########################################################################################################################################################

#Procedure
zones = gpd.read_file(INPUT_FILE_NAME)

longest_lines = []
zone_num = 1
for polygon in list(zones.geometry):

    print(f"Loading zone {zone_num}\n")

    #List all of the zone polygon's coordinates
    poly_coords = str(mapping(polygon)["coordinates"])

    #Extracts x, y coordinate floats from nested tuple format
    poly_xys = re.findall("\d+\.\d+", poly_coords)
    poly_xys = zip(poly_xys[::2],poly_xys[1::2])

    lines = []
    for p1,p2 in combinations(poly_xys,2):

        #Loops through all combinations of vertices creating a line for each one
        line = LineString([(float(p1[0]),float(p1[1])), (float(p2[0]),float(p2[1]))])

        #Only includes length of the line that passes through the zone
        line = line.intersection(polygon)

        #Sometimes the line with the longest internal distance will not reach the other point within the zone and create a line, point collection that cannot be output
        #To avoid this these instances are discarded
        if line.geom_type == 'GeometryCollection':
            continue

        lines.append(line)
    
    #Find the longest line and add to the list
    longest_lines.append(max(lines, key=lambda x: x.length))
    zone_num +=1

#Replace polygon geometries with the new line geometries
zones.geometry=longest_lines

zones.to_file(OUTPUT_FILE_NAME)