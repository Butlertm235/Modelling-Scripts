#Title: TrafficMaster queue counts
#Description: Processes TM data to count the number of queueing occurrences from speeds
#version: 1.0
#Author: Eve Rogers, email: Eve.Rogers@wsp.com
#Date: 06/07/2022

##Read required packages
if (!("data.table" %in% installed.packages())) {install.packages("data.table")}
library(data.table)
if (!("dplyr" %in% installed.packages())) {install.packages("dplyr")}
library(dplyr)
if (!("measurements" %in% installed.packages())) {install.packages("measurements")}
library(measurements)
if (!("sf" %in% installed.packages())) {install.packages("sf")}
library(sf)
if (!("tmap" %in% installed.packages())) {install.packages("tmap")}
library(tmap)
if (!("leaflet" %in% installed.packages())) {install.packages("leaflet")}
library(leaflet)
if (!("htmlwidgets" %in% installed.packages())) {install.packages("htmlwidgets")}
library(htmlwidgets)

##Start up - cleans up memory and removes variables from environment
rm(list=ls(all=TRUE)) #remove variables from global environment
cat("\014") #clear the console
gc() #free up memory

##Inputs
year_range = "2021" ##Either '2021', '2022' or, '2021-2022'
tm_file_path = paste0("./inputs/INRIXData/",year_range,"/") ##Location of raw TM data
link_list_name = ""    #link_ids to filter. Include 'link_id' header. Set to "" if no filtering
date_name = "./inputs/Neutral Day_dates_2021-2022.csv" #2021 dates, including neutral days and BST
links_name      = "./inputs/SW_ITN_SRN.gpkg"

##Outputs
outfile_raw = paste0("./outputs/TM_raw_motorway_",year_range,".csv") #file to write raw filtered data
outfile_name = paste0("Queue duration by day_",year_range,".html")
out_rda = paste0("./outputs/rda/Queue duration by day_",year_range,".rda") #write leaflet to r data file
out_shp = paste0("./outputs/shp/queue/",year_range,"_queue_daily") #file path and start naming convention to write shapefiles

##Read data
dates = fread(date_name) %>%
  mutate(Date = as.Date(Date, format="%d/%m/%Y"))
names(dates)[names(dates) == "Date"] <- "date_1"

links = read_sf(links_name) %>%
  st_zm(drop = TRUE, what = "ZM") #make sure data doesn't have z coords

if (file.exists(outfile_raw)) { #added so filtering for required links is only required on the first run
  df_raw = fread(outfile_raw) #read filtered data if already processed
} else {
  
  files = list.files(path = tm_file_path, pattern = "NWEngland_.*\\.csv") #list all TM files
  df_raw = data.frame() #define empty data frame
  
  for (i in 1:length(files)) { #loop through each tm file
    df_in = fread(paste0(tm_file_path, files[i]), select = c(1, 3, 7, 8, 4, 10)) %>% #read listed columns of full data
      mutate(date_1 = as.Date(date_1, format="%d/%m/%Y"))
    if (link_list_name != "") {
      link_list = fread(link_list_name) #read required links
      df_in = inner_join(df_in, link_list, by=c("link_id")) #filter for required links
    }
    df_raw = bind_rows(df_raw,df_in) #bind tm files to 1 file
    rm(df_in)
  }
  
  ##Write combined raw tm data for area
  fwrite(df_raw, outfile_raw)
}

speed_band = data.frame(speed_group = 1:4, speed_band = c("<20mph", "20-40mph", "40-60mph", ">60mph")) %>% #define speed bands
  mutate(speed_band = factor(speed_band, levels = c("<20mph", "20-40mph", "40-60mph", ">60mph"))) #ordering in legend

##Process by weekday
day_lookup = c("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")


#df_raw = inner_join(df_raw, dates,  by = c("date_1")) #add date info
df_raw = merge(df_raw, dates, by="date_1")
df_raw = mutate(df_raw, time_per = case_when(Bst==0 ~ time_per, Bst==1 ~ as.integer((time_per+4)%%96))) %>% #BST correction
  mutate(speed_mph = conv_unit(len_m / avg_jt, "m_per_sec", "mph")) #calculate speed in mph

##Map definitions
tmap_mode("view")
queue_map = tm_basemap(providers$CartoDB.Positron) #background map
group_name = c() #define empty vector

##Loop through days
count = 1
for (day in 1:1) { #loop through each day of the week
  day = 2
  group_name = c(group_name, day_lookup[day]) #vector of names
  df_day = filter(df_raw, Weekday == day) #filter for weekday
                      
  df_day = df_day[rep(1:nrow(df_day), df_day$N), ] %>% #replicate values by number of observations
    select(-N) %>%
    #group_by(link_id, road_type, time_per) %>% #results disaggregated by link and tp
    group_by(link_id, time_per) %>% #UPDATE
    dplyr::summarise(N = n(), 
              av_speed_mph = median(as.numeric(speed_mph)), #calculate median speed
              std_dev = sd(speed_mph), #calculate standard deviation
              len_m = median(as.numeric(len_m))) %>% #median length - should be no variation
    mutate(speed_group = findInterval(av_speed_mph, c(0, 20, 40, 60, Inf))) %>% #speed bands
    inner_join(speed_band)
    df_day = inner_join(links, df_day, by = "link_id")
  
  df_delay_len = filter(df_day, ifelse(RoadType == "Mainline", av_speed_mph < 30, av_speed_mph < 10)) %>% #filter speeds
    group_by(link_id) %>% #results disaggregated by link
    dplyr::summarise(av_speed_mph_c = sum(as.numeric(av_speed_mph * N)) / sum(as.numeric(N)), #weighted average
              N_c = sum(N), count_15mins = n()) %>%
    dplyr::rename(av_speed_mph = av_speed_mph_c, N = N_c) %>%
    mutate(delay_len_hrs = count_15mins/4) #convert to hours
  
  #df_day_new = select(df_day, link_id, RoadType, geom, av_speed_mph, N) %>%
  #  group_by(link_id) %>%
  #  if (RoadTpye == "Mainline")
  #  dplyr::summarise(av_speed_mph_c = sum(as.numeric(av_speed_mph * N)) / sum(as.numeric(N)), 
  #                          N_c = sum(N), count_15mins = n()) %>%
  #  dplyr::rename(av_speed_mph = av_speed_mph_c, N = N_c) %>%
  #  mutate(delay_len_hrs = count_15mins/4)
  
  
  #links_delay_len = inner_join(links, df_delay_len, by = "link_id") #join data to shapefile
  
  if (count == 1) { #legend only included in first loop
    queue_map = queue_map + 
      #tm_shape(links_delay_len) + #add shapefile to map
      tm_shape(df_delay_len) + #UPDATE
      tm_lines(col = "delay_len_hrs", lwd = 4, breaks = c(0.25,1,2,3,4,Inf), palette = colours() [c(142, 148, 90, 134, 137)], #categorised display
               group = day_lookup[day], legend.col.show = TRUE, title.col = "2021 Duration of delay", 
               legend.format = c(fun = function(x) paste0(x, " hours")), #formatting of display
               popup.vars = c("Number of observations" = "N", "Average speed when below 30/10mph" = "av_speed_mph", #add info when links selected
                              "Duration of delay below 30/10mph (hours)" = "delay_len_hrs"), 
               popup.format = list(av_speed_mph = list(digits = 0))) #formatting of display
  } else { #same as above, just with legend = FALSE
    queue_map = queue_map + 
      #tm_shape(links_delay_len) +
      tm_shape(df_delay_len) + #UPDATE
      tm_lines(col = "delay_len_hrs", lwd = 4, breaks = c(0.25,1,2,3,4,Inf), palette = colours() [c(142, 148, 90, 134, 137)], 
               group = day_lookup[day], legend.col.show = FALSE, title.col = "2021 Duration of delay", 
               legend.format = c(fun = function(x) paste0(x, " hours")), 
               popup.vars = c("Number of observations" = "N", "Average speed when below 30/10mph" = "av_speed_mph", 
                              "Duration of delay below 30/10mph (hours)" = "delay_len_hrs"), 
               popup.format = list(av_speed_mph = list(digits = 0)))
  }
  
  #write_sf(links_delay_len, paste0(out_shp, "_", day_lookup[day],".shp"))
  write_sf(df_delay_len, paste0(out_shp, "_", day_lookup[day],".shp")) #UPDATE
  
  count = count + 1
  
}

##Create and write interactive map
#leaflet = tmap_leaflet(queue_map) %>% #create leaflet
#  addLayersControl(
#    baseGroups = group_name, #add selectable elements to leaflet
#    options = layersControlOptions(collapsed = FALSE) #options panel not hidden
#  )
#leaflet

##Save to file
#saveWidget(leaflet, outfile_name, selfcontained = TRUE)
#file.copy(outfile_name, "./outputs", overwrite=TRUE)
#file.remove(outfile_name)
#save(leaflet, file = out_rda)
