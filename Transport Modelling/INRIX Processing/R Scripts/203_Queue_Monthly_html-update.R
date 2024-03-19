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

month_lookup = c("January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December")

##Map definitions
tmap_mode("view")
queue_map = tm_basemap(providers$CartoDB.Positron) #background map
group_name = c() #define empty vector

count = 1
for (month in 1:9) { 
  group_name = c(group_name, month_lookup[month]) #vector of names
  file_location = paste0("C:/Users/UKTHM001/Documents/70091126 - SW Network Delay Tool/INRIX processing/outputs/shp/queue/2022_queue_monthly_",month_lookup[month],".shp")
  df_delay_len = read_sf(file_location)
   
 if (count == 1) { #legend only included in first loop
   queue_map = queue_map + 
     #tm_shape(links_delay_len) + #add shapefile to map
     tm_shape(df_delay_len) + #UPDATE
     tm_lines(col = "dly_ln_", lwd = 4, breaks = c(0.25,1,2,3,4,Inf), palette = colours() [c(142, 148, 90, 134, 137)], #categorised display
              group = month_lookup[month], legend.col.show = TRUE, title.col = "2022 Duration of delay", 
              legend.format = c(fun = function(x) paste0(x, " hours")), #formatting of display
              popup.vars = c("Number of observations" = "N", "Average speed when below 30/10mph" = "av_spd_", #add info when links selected
                             "Duration of delay below 30/10mph (hours)" = "dly_ln_"), 
              popup.format = list(av_spd_ = list(digits = 0))) #formatting of display
 } else { #same as above, just with legend = FALSE
   queue_map = queue_map + 
     #tm_shape(links_delay_len) +
     tm_shape(df_delay_len) + #UPDATE
     tm_lines(col = "dly_ln_", lwd = 4, breaks = c(0.25,1,2,3,4,Inf), palette = colours() [c(142, 148, 90, 134, 137)], 
              group = month_lookup[month], legend.col.show = FALSE, title.col = "2022 Duration of delay", 
              legend.format = c(fun = function(x) paste0(x, " hours")), 
              popup.vars = c("Number of observations" = "N", "Average speed when below 30/10mph" = "av_spd_", 
                             "Duration of delay below 30/10mph (hours)" = "dly_ln_"), 
              popup.format = list(av_spd_ = list(digits = 0)))
 }
 
 count = count + 1
 
}

##Create and write interactive map
leaflet = tmap_leaflet(queue_map) %>% #create leaflet
  addLayersControl(
    baseGroups = group_name, #add selectable elements to leaflet
    options = layersControlOptions(collapsed = FALSE) #options panel not hidden
  )
#leaflet


outfile_name = paste0("Queue duration by month_2022.html")
out_rda = paste0("./outputs/rda/Queue duration by month_2022.rda") #write leaflet to r data file


##Save to file
saveWidget(leaflet, outfile_name, selfcontained = TRUE)
file.copy(outfile_name, "./outputs", overwrite=TRUE)
file.remove(outfile_name)
save(leaflet, file = out_rda)