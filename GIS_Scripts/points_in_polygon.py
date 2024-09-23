"""
Created September 2024

@author: UKSAC775
"""

import numpy as np
import pandas as pd
import geopandas as gpd
import shapely.geometry
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
#import fiona

def Points_in_polygon(points,geopackage,output_directory):

    """
    Reads in a data base of points, and returns the ids of those points that lie within a specified polygon
    Code is set up to work with specific formats/coordinate systems, see Parameters below. Should be simple to adapt code if needed.

    Parameters
    ----------
    points : dataframe
        Dataframe with the raw survey data.
        SET UP TO WORK WITH POINTS IN LAT/LONG FORMAT, WITH COLUMNS: ["id","longitude","latitude"]
    geopackage : string
        Name/location of the geopackage that the function should see if points are within.
        SET UP TO WORK WITH EASTING/NORTHING FORMAT (code converts this to lat/long)
    output_directory

    Returns
    -------
    data_points : dataframe       
        Dataframe with the ids of the points within the shape
    "inshape.csv" : csv file
        Csv file of the points dataframe, with a TRUE/FALSE column whether the point is in the shape

    """

    data=gpd.read_file(geopackage) #polygon geopackage

    data_points = points #list of sites with longitude and latitude columns

    data_polygon=data.explode() #multipolygon to polygon
    polygon1 = data_polygon.loc[0, 'geometry']

    polygon2=polygon1.to_crs(epsg=4326) #converts easting/northing to long/lat
    polygon=polygon2.iloc[0]

    data_points["inShape"] = np.nan

    for i in range(len(data_points)):
        inShape = shapely.geometry.Point(data_points["longitude"].iloc[i],data_points["latitude"].iloc[i]).within(polygon)
        data_points["inShape"].iloc[i]=inShape

    data_points.to_csv(output_directory+"inShape.csv")

    return data_points[data_points.inShape]["id"]

