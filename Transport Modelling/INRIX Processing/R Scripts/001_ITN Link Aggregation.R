#Title: Link location aggregation
#Description: Aggregates ITN links by location and writes geopackage
#version: 0.1
#Author: Eve Rogers, email: Eve.Rogers@wsp.com
#Date: 14/11/2022

##Read required packages
if (!("sf" %in% installed.packages())) {install.packages("sf")}
library(sf)
if (!("dplyr" %in% installed.packages())) {install.packages("dplyr")}
library(dplyr)

##Inputs
links_name = "./inputs/SW_ITN_SRN.gpkg"
location_out = "./outputs/SW_ITN_SRN_locations.gpkg"
location_point_out = "./outputs/SW_ITN_SRN_locations_points.gpkg"

##Read links shapefile and aggregate by location
links = read_sf(links_name) %>%
  st_zm(drop = TRUE, what = "ZM") #make sure data doesn't have z coords

links_agg = group_by(links, Location) %>%
  dplyr::summarise()

link_midpoint = st_centroid(links_agg)

##Write to geopackage
write_sf(links_agg, location_out)
write_sf(link_midpoint, location_point_out)