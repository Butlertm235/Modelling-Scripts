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


year_range = "2021" ##Either '2021', '2022' 
weekday_type = "Neutral Day" #Either 'neutral' or 'School Holiday'

if (weekday_type == "Neutral Day") {
  peak_times_list = list(c("06:00", "07:00"), c("07:00", "08:00"), c("08:00", "09:00"), c("09:00", "10:00"), 
                         c("15:00", "16:00"), c("16:00", "17:00"), c("17:00", "18:00"), c("18:00", "19:00")) #create a list with AM, PM as separate vectors
} else {
  (peak_times_list = list(c("09:00", "10:00"), c("10:00", "11:00"), c("11:00", "12:00"), c("12:00", "13:00"), c("13:00", "14:00"),
                          c("14:00", "15:00"), c("15:00", "16:00"), c("16:00", "17:00"), c("17:00", "18:00")))
}


##Map definitions
tmap_mode("view")
speed_map = tm_basemap(providers$CartoDB.Positron) #background map
group_name = c() #define empty vector

count = 1
for (peak_times in peak_times_list) { #loop through each defined time period
  
  ##Speed map for time period
  time_name = paste0(peak_times[1], "-", peak_times[2]) #naming convention
  time_name_shp = gsub(":", "", time_name)
  group_name = c(group_name, time_name) #vector of names
  
  file_location = paste0("C:/Users/UKTHM001/Documents/70091126 - SW Network Delay Tool/INRIX processing/outputs/shp/speed_SD/",year_range,"_AvgSpeed_SD_",weekday_type,"_",time_name_shp,".shp")
  links_analysis = read_sf(file_location)
  
  if (count == 1) { #legend only included in first loop
    speed_map = speed_map +
      tm_shape(links_analysis) + #add shapefile to map
      tm_lines(col = "speed_band", lwd = 4, 
               palette = c("<10mph" = '#FF0000', "10-20mph" = '#FF5200', "20-30mph" = '#FFC017', "30-40mph" = '#FFFF38', "40-50mph" = '#B3D513', "50-60mph" = '#5BAA00', ">60mph" = '#0B8000'), #categorised display
               group = time_name, legend.col.show = TRUE, title.col = "2021 Average Speeds", 
               popup.vars = c("Number of observations" = "N", "Average speed (mph)" = "av_speed_mph", 
                              "Speed standard deviation (mph)" = "std_dev", "Speed band" = "speed_band"), #add info when links selected
               popup.format = list(std_dev = list(digits = 0), av_speed_mph = list(digits = 0))) #formatting of display
  } else { #same as above, just with legend = FALSE
    speed_map = speed_map +
      tm_shape(links_analysis) +
      tm_lines(col = "speed_band", lwd = 4, 
               palette = c("<10mph" = '#FF0000', "10-20mph" = '#FF5200', "20-30mph" = '#FFC017', "30-40mph" = '#FFFF38', "40-50mph" = '#B3D513', "50-60mph" = '#5BAA00', ">60mph" = '#0B8000'), 
               group = time_name, legend.col.show = FALSE, title.col = "2021 Average Speeds", 
               popup.vars = c("Number of observations" = "N", "Average speed (mph)" = "av_speed_mph", 
                              "Speed standard deviation (mph)" = "std_dev", "Speed band" = "speed_band"), 
               popup.format = list(std_dev = list(digits = 0), av_speed_mph = list(digits = 0)))
  }
  count = count + 1
}

##Create and write interactive map
leaflet = tmap_leaflet(speed_map) %>% #create leaflet
  addLayersControl(
    baseGroups = group_name, #add selectable elements to leaflet
    options = layersControlOptions(collapsed = FALSE) #options panel not hidden
  )
#leaflet

outfile_name = paste0("Average Speeds_",weekday_type,"_",year_range,"-Test.html")
out_rda = paste0("./outputs/rda/Average Speeds_",weekday_type,"_",year_range,"-Test.rda") #write leaflet to r data file

##Save to file
saveWidget(leaflet, outfile_name, selfcontained = TRUE) 
file.copy(outfile_name, "./outputs", overwrite=TRUE) 
file.remove(outfile_name)
save(leaflet, file = out_rda)