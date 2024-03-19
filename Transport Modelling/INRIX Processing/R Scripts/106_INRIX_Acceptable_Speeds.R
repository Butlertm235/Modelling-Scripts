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
if (!("tidyverse" %in% installed.packages())) {install.packages("tidyverse")}
library(tidyverse)

##Start up - cleans up memory and removes variables from environment
rm(list=ls(all=TRUE)) #remove variables from global environment
cat("\014") #clear the console
gc() #free up memory

##Inputs
year_range = "2021" ##Either '2021', '2022'
weekday_type = "Neutral Day"  #Either 'Neutral Day' or 'School Holiday'

tm_file_path = paste0("./inputs/INRIXData/",year_range,"/") ##Location of raw TM data
link_list_name = ""    #link_ids to filter. Include 'link_id' header. Set to "" if no filtering
date_name = paste0("./inputs/",weekday_type,"_dates_2021-2022.csv") #2021 dates, including neutral days and BST
links_name      = "./inputs/SW_ITN_SRN.gpkg"
if (weekday_type == "Neutral Day") {
  peak_times_list = list(c("06:00", "07:00"), c("07:00", "08:00"), c("08:00", "09:00"), c("09:00", "10:00"), 
                         c("15:00", "16:00"), c("16:00", "17:00"), c("17:00", "18:00"), c("18:00", "19:00")) #create a list with AM, PM as separate vectors
} else {
  peak_times_list = list(c("06:00", "07:00"), c("07:00", "08:00"), c("08:00", "09:00"), c("09:00", "10:00"), 
                         c("10:00", "11:00"), c("11:00", "12:00"), c("12:00", "13:00"), c("13:00", "14:00"),
                         c("14:00", "15:00"), c("15:00", "16:00"), c("16:00", "17:00"), c("17:00", "18:00"),
                         c("18:00", "19:00"))
}
offpeak_times   = c("00:00","06:00") #a single vector

##Outputs
outfile_raw = paste0("./outputs/TM_raw_motorway_",year_range,".csv") #file to write raw filtered data
outfile_name = paste0("Acceptable_Speeds_",weekday_type,"_",year_range,".html")
out_rda = paste0("./outputs/rda/Acceptable Speeds_",weekday_type,"_",year_range,".rda") #write leaflet to r data file
out_shp = paste0("./outputs/shp/AcceptableSpeed/",year_range,"_ASpeed_SD_",weekday_type) #file path and start naming convention to write shapefiles

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

##Read data
dates = fread(date_name, select = c(1, 4, 5)) %>%
  mutate(Date = as.Date(Date, format="%d/%m/%Y")) %>%
  filter(SchoolDay == 1) #filter neutral days
names(dates)[names(dates) == "Date"] <- "date_1"

links = read_sf(links_name) %>%
  st_zm(drop = TRUE, what = "ZM") #make sure data doesn't have z coords

df_raw = fread(outfile_raw) #read filtered data if already processed

speed_band = data.frame(acc_speed_group = 1:5, speed_band = c("<55%", "55-65%", "65-75%", "75-85%", ">85%")) %>%
  mutate(speed_band = factor(speed_band, levels = c("<55%", "55-65%", "65-75%", "75-85%", ">85%")))

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
         end_time = paste0(sprintf("%02d", floor((time_per+1)/4)%%24), ":", sprintf("%02d", 15*((time_per+1)%%4)))) #add times

##Map definitions
tmap_mode("view")
speed_map = tm_basemap(providers$CartoDB.Positron) #background map
group_name = c() #define empty vector
legend_title = paste0(year_range, " ", weekday_type, " Acceptable Speeds")

count = 1
for (peak_times in peak_times_list) { #loop through each defined time period
  
  df_filter = speed_summarise(df_neutral, peak_times) #above defined function
  df_filter_off = speed_summarise(df_neutral, offpeak_times)
  names(df_filter_off)[names(df_filter_off) == "av_speed_mph"] <- "av_offspeed_mph"
  names(df_filter_off)[names(df_filter_off) == "N"] <- "N_off"
  names(df_filter_off)[names(df_filter_off) == "std_dev"] <- "std_dev_off"
  df_filter = inner_join(df_filter, df_filter_off, by = 'link_id') %>%
    mutate(acc_speed = av_speed_mph / av_offspeed_mph) %>%
    mutate(acc_speed_group = findInterval(acc_speed, c(0, 0.55, 0.65, 0.75, 0.85, Inf))) %>%
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
               palette = c("<55%" = '#FF0000', "55-65%" = '#FFC017', "65-75%" = '#FFFF38', "75-85%" = '#87BF05', ">85%" = '#0B8000'), #categorised display
               group = time_name, legend.col.show = TRUE, title.col = legend_title, 
               popup.vars = c("Peak Average speed (mph)" = "av_speed_mph", 
                              "Off Peak Average Speed" = "av_offspeed_mph", "Fraction of Free Flow Speed" = "acc_speed"), #add info when links selected
               popup.format = list(av_speed_mph = list(digits = 0), av_offspeed_mph = list(digits = 0), acc_speed = list(digits = 2))) #formatting of display
  } else { #same as above, just with legend = FALSE
    speed_map = speed_map +
      tm_shape(links_analysis) +
      tm_lines(col = "speed_band", lwd = 4, 
               palette = c("<55%" = '#FF0000', "55-65%" = '#FFC017', "65-75%" = '#FFFF38', "75-85%" = '#87BF05', ">85%" = '#0B8000'), 
               group = time_name, legend.col.show = FALSE, title.col = legend_title, 
               popup.vars = c("Peak Average speed (mph)" = "av_speed_mph", 
                              "Off Peak Average Speed" = "av_offspeed_mph", "Fraction of Free Flow Speed" = "acc_speed"), 
               popup.format = list(av_speed_mph = list(digits = 0), av_offspeed_mph = list(digits = 0), acc_speed = list(digits = 2)))
  }
  
  write_sf(links_analysis, paste0(out_shp, "_", time_name_shp,".shp"))
  
  count = count + 1
  
}

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
