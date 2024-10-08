#Title: TrafficMaster Speed Bands
#Description: Processes TM data and creates html speed banded plot for defined dates. Includes a loop for all TM files.
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

##################################Setting up Inputs/Outputs##################################

##Inputs
year_range = "2022" ##Either '2021', '2022' or, '2021-2022'
#weekday_type = "Neutral Day"  
weekday_type = "Neutral Day" #Either 'Neutral' or 'School Holiday'

tm_file_path = paste0("./inputs/INRIXData/",year_range,"/") ##Location of raw TM data
link_list_name = "./inputs/SW_ITN_Manadon.csv"    #link_ids to filter. Include 'link_id' header. Set to "" if no filtering
date_name = paste0("./inputs/",weekday_type,"_dates_2021-2022.csv") #2021 dates, including neutral days and BST
links_name      = "./inputs/SW_ITN_Manadon.gpkg"
if (weekday_type == "Neutral Day") {
  peak_times_list = list(c("06:00", "07:00"), c("07:00", "08:00"), c("08:00", "09:00"), c("09:00", "10:00"), 
                         c("15:00", "16:00"), c("16:00", "17:00"), c("17:00", "18:00"), c("18:00", "19:00")) #create a list with AM, PM as separate vectors
} else {
  peak_times_list = list(c("06:00", "07:00"), c("07:00", "08:00"), c("08:00", "09:00"), c("09:00", "10:00"), 
                         c("10:00", "11:00"), c("11:00", "12:00"), c("12:00", "13:00"), c("13:00", "14:00"),
                         c("14:00", "15:00"), c("15:00", "16:00"), c("16:00", "17:00"), c("17:00", "18:00"),
                         c("18:00", "19:00"))
}


##Outputs
outfile_raw = paste0("./outputs/TM_raw_Manadon_",year_range,".csv") #file to write raw filtered data
outfile_name = paste0("Average Speeds_Manadon_",weekday_type,"_",year_range,".html")
out_rda = paste0("./outputs/rda/Average Speeds_Manadon_",weekday_type,"_",year_range,".rda") #write leaflet to r data file
out_shp = paste0("./outputs/shp/speed_SD/",year_range,"_AvgSpeed_SD_Manadon_",weekday_type) #file path and start naming convention to write shapefiles
outfile_speed = paste0("./outputs/INRIX_Manadon_",year_range,".csv")
##################################Functions##################################

##Define functions
combine_sd = function (xbar, sd, n) { #function to combine multiple standard deviations
  if (length(xbar) != length(sd) | length(xbar) != length(n) | length(sd) != length(n)) #check for errors
    stop("Vector lengths are different.")
  var = sd^2 #calculate variance
  sum_of_squares = sum((n - 1) * var + n * xbar^2) #calculate sum of squared speeds
  grand_mean = sum(n * xbar)/sum(n) #calculate weighted mean
  combined_var = (sum_of_squares - sum(n) * grand_mean^2)/(sum(n) - 1) #combine variances
  combined_sd = sqrt(combined_var)#combine sd
  #return(c(grand_mean, combined_var, combined_sd))
}

speed_summarise = function(df_sum, times){ #filter for observations in time period and average data by link
  filter(df_sum, if (times[1] < times[2]) start_time >= times[1] & end_time <= times[2] & end_time >"00:00" #filter time periods within defined tp
         else (start_time >= times[1] | start_time < times[2]) & (end_time > times[1] | end_time <= times[2]) # deals with overnight
  ) %>%
    select(-start_time, -end_time) %>%
    group_by(link_id) %>%
    dplyr::summarise(av_speed_mph_c = sum(as.numeric(av_speed_mph * N)) / sum(as.numeric(N)), #weighted average
              N_c=sum(as.numeric(N)), 
              std_dev_c = combine_sd(av_speed_mph, std_dev, N)) %>% ##relies on above function
    dplyr::rename(av_speed_mph = av_speed_mph_c, N = N_c, std_dev = std_dev_c)
}

##################################Reading in data##################################

##Read data
dates = fread(date_name, select = c(1, 4, 5)) %>%
  mutate(Date = as.Date(Date, format="%d/%m/%Y")) %>%
  filter(SchoolDay == 1) #filter neutral days
names(dates)[names(dates) == "Date"] <- "date_1"

links = read_sf(links_name) %>%
  st_zm(drop = TRUE, what = "ZM") #make sure data doesn't have z coords

if (file.exists(outfile_raw)) { #added so filtering for required links is only required on the first run
  df_raw = fread(outfile_raw) #read filtered data if already processed
} else {
  
  files = list.files(path = tm_file_path, pattern = "*\\.csv") #list all TM files
  df_raw = data.frame() #define empty data frame
  
  for (i in 1:length(files)) { #loop through each tm file
    df_in = fread(paste0(tm_file_path, files[i]), select = c(1, 3, 4, 7, 8, 10)) %>% #read listed columns of full data
      #mutate(date_1 = as.numeric(date_1)) %>%
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

##################################Formatting data for analysis##################################

speed_band = data.frame(speed_group = 1:7, speed_band = c("<10mph", "10-20mph", "20-30mph", "30-40mph", "40-50mph", "50-60mph", ">60mph")) %>% #define speed bands
  mutate(speed_band = factor(speed_band, levels = c("<10mph", "10-20mph", "20-30mph", "30-40mph", "40-50mph", "50-60mph", ">60mph"))) #ordering in legend

##Process for neutral days
#df_neutral = inner_join(df_raw, dates,  by = c("date_1" = "Date")) #filter for neutral days
df_neutral = merge(df_raw, dates, by="date_1")
df_neutral = mutate(df_neutral, time_per = case_when(Bst==0 ~ time_per, Bst==1 ~ as.integer((time_per+4)%%96))) %>% #BST correction
  mutate(speed_mph = conv_unit(len_m / avg_jt, "m_per_sec", "mph")) #calculate speed in mph

df_neutral = df_neutral[rep(1:nrow(df_neutral), df_neutral$N), ] %>% #replicate values by number of observations
  select(-N) %>%
  group_by(link_id, time_per) %>% #results disaggregated by link and tp
  dplyr::summarise(N = n(), 
            av_speed_mph = median(as.numeric(speed_mph)), #calculate median speed
            std_dev = sd(speed_mph), #calculate standard deviation
            len_m = median(as.numeric(len_m))) %>% #median length - should be no variation
  mutate(start_time = paste0(sprintf("%02d", floor(time_per/4)), ":", sprintf("%02d", 15*(time_per%%4))),
         end_time = paste0(sprintf("%02d", floor((time_per+1)/4)%%24), ":", sprintf("%02d", 15*((time_per+1)%%4)))) %>% #add times
  mutate(speed_group = findInterval(av_speed_mph, c(0, 10, 20, 30, 40, 50, 60, Inf))) %>% #speed bands
  inner_join(speed_band)

fwrite(df_neutral,outfile_speed)
##################################Setting up mapping##################################

##Map definitions
tmap_mode("view")
speed_map = tm_basemap(providers$CartoDB.Positron) #background map
group_name = c() #define empty vector
legend_title = paste0(year_range, " ", weekday_type, " Average Speeds")

##################################Analysis of data##################################

##Loop through time periods
count = 1
for (peak_times in peak_times_list) { #loop through each defined time period
  
  df_filter = speed_summarise(df_neutral, peak_times) %>% #above defined function
    mutate(speed_group = findInterval(av_speed_mph, c(0, 10, 20, 30, 40, 50, 60, Inf))) %>% #speed banding
    inner_join(speed_band)
  links_analysis = inner_join(links, df_filter, by = "link_id") #join data to shapefile
  
  ##Speed map for time period
  time_name = paste0(peak_times[1], "-", peak_times[2]) #naming convention
  time_name_shp = gsub(":", "", time_name)
  group_name = c(group_name, time_name) #vector of names
  
  if (count == 1) { #legend only included in first loop
    speed_map = speed_map +
      tm_shape(links_analysis) + #add shapefile to map
      tm_lines(col = "speed_band", lwd = 4, 
               palette = c("<10mph" = '#FF0000', "10-20mph" = '#FF5200', "20-30mph" = '#FFC017', "30-40mph" = '#FFFF38', "40-50mph" = '#B3D513', "50-60mph" = '#5BAA00', ">60mph" = '#0B8000'), #categorised display
               group = time_name, legend.col.show = TRUE, title.col = legend_title, 
               popup.vars = c("Number of observations" = "N", "Average speed (mph)" = "av_speed_mph", 
                              "Speed standard deviation (mph)" = "std_dev", "Speed band" = "speed_band"), #add info when links selected
               popup.format = list(std_dev = list(digits = 0), av_speed_mph = list(digits = 0))) #formatting of display
  } else { #same as above, just with legend = FALSE
    speed_map = speed_map +
      tm_shape(links_analysis) +
      tm_lines(col = "speed_band", lwd = 4, 
               palette = c("<10mph" = '#FF0000', "10-20mph" = '#FF5200', "20-30mph" = '#FFC017', "30-40mph" = '#FFFF38', "40-50mph" = '#B3D513', "50-60mph" = '#5BAA00', ">60mph" = '#0B8000'), 
               group = time_name, legend.col.show = FALSE, title.col = legend_title, 
               popup.vars = c("Number of observations" = "N", "Average speed (mph)" = "av_speed_mph", 
                              "Speed standard deviation (mph)" = "std_dev", "Speed band" = "speed_band"), 
               popup.format = list(std_dev = list(digits = 0), av_speed_mph = list(digits = 0)))
  }
  
  write_sf(links_analysis, paste0(out_shp, "_", time_name_shp,".shp"))
  
  count = count + 1
  
}

##################################Outputing data into multiple formats##################################

##Create and write interactive map
leaflet = tmap_leaflet(speed_map) %>% #create leaflet
  addLayersControl(
    baseGroups = group_name, #add selectable elements to leaflet
    options = layersControlOptions(collapsed = FALSE) #options panel not hidden
  )
#leaflet

##Save to file
saveWidget(leaflet, outfile_name, selfcontained = TRUE) 
file.copy(outfile_name, "./outputs", overwrite=TRUE) 
file.remove(outfile_name)
save(leaflet, file = out_rda)
