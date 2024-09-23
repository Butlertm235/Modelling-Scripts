###################################Setting up the libraries and input/output locations###################################

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
if (!("DT" %in% installed.packages())) {install.packages("DT")}
library(DT)
if (!("crosstalk" %in% installed.packages())) {install.packages("crosstalk")}
library(crosstalk)

##Start up - cleans up memory and removes variables from environment
rm(list=ls(all=TRUE)) #remove variables from global environment
cat("\014") #clear the console
gc() #free up memory

##Inputs
year_range = "2021" ##Either '2021', '2022'
tm_file_path = paste0("./inputs/INRIXData/",year_range,"/") ##Location of raw TM data
link_list_name = "./inputs/SW_ITN_SRN_v2.csv"    #link_ids to filter. Include 'link_id' header. Set to "" if no filtering
date_name = "./inputs/Neutral Day_dates_2021-2022.csv" #2021 dates, including neutral days and BST
links_name      = "./inputs/SW_ITN_SRN.gpkg"

queue_filter = "Slip Junction" #'Mainline or Slip Junction"

##Outputs
outfile_raw = paste0("./outputs/TM_raw_motorway_",year_range,".csv") #file to write raw filtered data
if (queue_filter == "Mainline") {
  outfile_rank_link = paste0("./outputs/TM_queue ranking_",year_range,"_mainline_raw.csv")
} else {
  outfile_rank_link = paste0("./outputs/TM_queue ranking_",year_range,"_slip_junction_raw.csv")
}
outfile_rank_loc_main = paste0("./outputs/TM_queue ranking_mainline_aggregated_",year_range,".csv")
outfile_rank_loc_other = paste0("./outputs/TM_queue ranking_slip_junction_aggregated_",year_range,".csv")

###################################Reading in the data###################################

dates = fread(date_name) %>%
  mutate(Date = as.Date(Date, format="%d/%m/%Y"))
names(dates)[names(dates) == "Date"] <- "date_1"

links = read_sf(links_name) %>%
  st_zm(drop = TRUE, what = "ZM") #make sure data doesn't have z coords



df_in = fread(outfile_raw) #read filtered data if already processed

link_location = fread(link_list_name) %>%
  select(link_id, Location, RoadType)

###################################Reducing Data size through filter###################################
if (queue_filter == "Mainline") {
  df_in = inner_join(df_in, link_location, by = "link_id")
  df_in = filter(df_in, RoadType == "Mainline" | RoadType == "At grade SRN A Road")
  
} else{ 
  df_in = inner_join(df_in, link_location, by = "link_id")
  df_in = filter(df_in, RoadType != "Mainline" | RoadType != "At grade SRN A Road")
  
}

#df_in = inner_join(df_in, link_location, by = "link_id")
#df_mainline = filter(df_in, RoadType == "Mainline" | RoadType == "At grade SRN A Road")
#df_sliproad = filter(df_in, RoadType != "Mainline" | RoadType != "At grade SRN A Road")

###################################Converting data into correct format###################################

# = inner_join(df_in, dates,  by = c("date_1" = "Date")) #add date info
df_in = merge(df_in, dates, by = "date_1")
df_in = mutate(df_in, time_per = case_when(Bst==0 ~ time_per, Bst==1 ~ as.integer((time_per+4)%%96))) %>% #BST correction
  mutate(speed_mph = conv_unit(len_m / avg_jt, "m_per_sec", "mph")) %>% #calculate speed in mph
  select(-date_1, -Weekday, -Month, -SchoolDay, -Bst)

gc()
df_in = df_in[rep(1:nrow(df_in), df_in$N), ] %>% #replicate values by number of observations
  select(-N) %>%
  group_by(link_id, time_per) %>% #results disaggregated by link and tp
  dplyr::summarise(N = n(), 
                   av_speed_mph = median(as.numeric(speed_mph)), #calculate median speed
                   mean_speed_mph = mean(as.numeric(speed_mph)), #calculate mean speed
                   std_dev = sd(speed_mph), #calculate standard deviation
                   len_m = median(as.numeric(len_m))) #median length - should be no variation
gc()

###################################Calculating Average Speed###################################

df_av_spd = group_by(df_in, link_id) %>%
  dplyr::summarise(av_speed_mph = sum(as.numeric(av_speed_mph * N)) / sum(as.numeric(N)), 
                   mean_speed_mph = sum(as.numeric(mean_speed_mph * N)) / sum(as.numeric(N))) %>% #weighted average
  select(link_id, av_speed_mph, mean_speed_mph) %>%
  left_join(link_location)

###################################Calculating Delay###################################

df_delay = inner_join(df_in, link_location, by = "link_id")

df_delay = filter(df_delay, ifelse(RoadType == "Mainline", av_speed_mph < 30, av_speed_mph < 10)) %>%
  group_by(link_id, RoadType) %>% 
  dplyr::summarise(av_speed_mph_q = sum(as.numeric(av_speed_mph * N)) / sum(as.numeric(N)), #weighted average
                   N_q = sum(N), count_15mins = n()) %>%
  mutate(delay_len_hrs = count_15mins/4) %>%
  left_join(df_av_spd) %>%
  select(link_id, Location, RoadType, N_q, av_speed_mph, mean_speed_mph, delay_len_hrs, av_speed_mph_q)

if (queue_filter == "Mainline") {
  df_delay_loc_main = filter(df_delay, RoadType == "Mainline" | RoadType == "At grade SRN A Road") %>%
    group_by(Location) %>%
    summarise(max_delay_hrs = max(delay_len_hrs), min_av_spd_mph = min(av_speed_mph), min_mean_spd_mph = min(mean_speed_mph), 
              min_av_spd_mph_q = min(av_speed_mph_q))
  
} else{ 
  df_delay_loc_other = filter(df_delay, RoadType != "Mainline" & RoadType != "At grade SRN A Road") %>%
    group_by(Location) %>%
    summarise(max_delay_hrs = max(delay_len_hrs), min_av_spd_mph = min(av_speed_mph), min_mean_spd_mph = min(mean_speed_mph), 
              min_av_spd_mph_q = min(av_speed_mph_q))
  
}

#df_delay_loc_main = filter(df_delay, RoadType == "Mainline" | RoadType == "At grade SRN A Road") %>%
#  group_by(Location) %>%
#  summarise(max_delay_hrs = max(delay_len_hrs), min_av_spd_mph = min(av_speed_mph), min_mean_spd_mph = min(mean_speed_mph), 
#            min_av_spd_mph_q = min(av_speed_mph_q))

#df_delay_loc_other = filter(df_delay, RoadType != "Mainline" & RoadType != "At grade SRN A Road") %>%
#  group_by(Location) %>%
#  summarise(max_delay_hrs = max(delay_len_hrs), min_av_spd_mph = min(av_speed_mph), min_mean_spd_mph = min(mean_speed_mph), 
#            min_av_spd_mph_q = min(av_speed_mph_q))

###################################Ranking the links based on delay###################################

##Rank
ranking_link = arrange(df_delay, desc(delay_len_hrs)) #arrange by highest length of average delay
ranking_link$rank = seq.int(nrow(ranking_link))

if (queue_filter == "Mainline") {
  ranking_loc_main = arrange(df_delay_loc_main, desc(max_delay_hrs))
  ranking_loc_main$rank = seq.int(nrow(ranking_loc_main))
  
  fwrite(ranking_link, outfile_rank_link)
  fwrite(ranking_loc_main, outfile_rank_loc_main)
  
} else{
  ranking_loc_other = arrange(df_delay_loc_other, desc(max_delay_hrs))
  ranking_loc_other$rank = seq.int(nrow(ranking_loc_other))
  
  fwrite(ranking_link, outfile_rank_link)
  fwrite(ranking_loc_other, outfile_rank_loc_other)
  
}
  
#ranking_loc_main = arrange(df_delay_loc_main, desc(max_delay_hrs))
#ranking_loc_main$rank = seq.int(nrow(ranking_loc_main))

#ranking_loc_other = arrange(df_delay_loc_other, desc(max_delay_hrs))
#ranking_loc_other$rank = seq.int(nrow(ranking_loc_other))

#fwrite(ranking_link, outfile_rank_link)
#fwrite(ranking_loc_main, outfile_rank_loc_main)
#fwrite(ranking_loc_other, outfile_rank_loc_other)

outfile_rank_link_mainline = paste0("./outputs/TM_queue ranking_",year_range,"_slip_junction_raw.csv")
outfile_rank_link_other = paste0("./outputs/TM_queue ranking_",year_range,"_mainline_raw.csv")
outfile_rank_link_combined = paste0("./outputs/TM_queue ranking_",year_range,".csv")

if (file.exists(outfile_rank_link_mainline) & file.exists(outfile_rank_link_other)) { #added so filtering for required links is only required on the first run
  df_mainline = fread(outfile_rank_link_mainline)
  df_other = fread(outfile_rank_link_other)
  df_combined = rbind(df_mainline, df_other)
  combined_ranking_link = arrange(df_combined, desc(delay_len_hrs))
  combined_ranking_link$rank = seq.int(nrow(combined_ranking_link))
  
  fwrite(combined_ranking_link, outfile_rank_link_combined)
} 