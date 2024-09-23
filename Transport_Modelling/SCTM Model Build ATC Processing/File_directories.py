import pandas as pd
import os
import datetime

###########CONSTANT PARAMETERS

PROGRAM_DIRECTORY = os.getcwd()

AM_15M_INTERVALS_730_PH = [str(datetime.time(hour = 7, minute = 30, second = 0)),str(datetime.time(hour = 7, minute = 45, second = 0)),str(datetime.time(hour = 8, minute = 00, second = 0)),str(datetime.time(hour = 8, minute = 15, second = 0))]
AM_15M_INTERVALS_8_PH = [str(datetime.time(hour = 8, minute = 00, second = 0)),str(datetime.time(hour = 8, minute = 15, second = 0)),str(datetime.time(hour = 8, minute = 30, second = 0)),str(datetime.time(hour = 8, minute = 45, second = 0))]
IP_15M_INTERVALS = [str(datetime.time(hour = 10, minute = 0, second = 0)),str(datetime.time(hour = 10, minute = 15, second = 0)),str(datetime.time(hour = 10, minute = 30, second = 0)),str(datetime.time(hour = 10, minute = 45, second = 0)),str(datetime.time(hour = 11, minute = 0, second = 0)),str(datetime.time(hour = 11, minute = 15, second = 0)),str(datetime.time(hour = 11, minute = 30, second = 0)),str(datetime.time(hour = 11, minute = 45, second = 0)),str(datetime.time(hour = 12, minute = 0, second = 0)),str(datetime.time(hour = 12, minute = 15, second = 0)),str(datetime.time(hour = 12, minute = 30, second = 0)),str(datetime.time(hour = 12, minute = 45, second = 0)),str(datetime.time(hour = 13, minute = 0, second = 0)),str(datetime.time(hour = 13, minute = 15, second = 0)),str(datetime.time(hour = 13, minute = 30, second = 0)),str(datetime.time(hour = 13, minute = 45, second = 0)),str(datetime.time(hour = 14, minute = 0, second = 0)),str(datetime.time(hour = 14, minute = 15, second = 0)),str(datetime.time(hour = 14, minute = 30, second = 0)),str(datetime.time(hour = 14, minute = 45, second = 0)),str(datetime.time(hour = 15, minute = 0, second = 0)),str(datetime.time(hour = 15, minute = 15, second = 0)),str(datetime.time(hour = 15, minute = 30, second = 0)),str(datetime.time(hour = 15, minute = 45, second = 0))]
PM_15M_INTERVALS_1700_PH = [str(datetime.time(hour = 17, minute = 00, second = 0)),str(datetime.time(hour = 17, minute = 15, second = 0)),str(datetime.time(hour = 17, minute = 30, second = 0)),str(datetime.time(hour = 17, minute = 45, second = 0))]


TIME_PERIODS = {"AM": AM_15M_INTERVALS_730_PH,
                "IP": IP_15M_INTERVALS,
                "PM": PM_15M_INTERVALS_1700_PH,
                }
############


########### FOT COPYING RAW DATA AND FOLDER STRUCTURE FROM CENTRAL DATA AFTER COPYING FROM GITHUB
#Update the data stored in central data whenever new data comes in, so that it can be copied using Create_DBs.copy_raw_data() after github repository is cloned
RAW_DATA_CENTRAL_DATA_DIRECTORY = r"\\uk.wspgroup.com\central data\Projects\70076xxx\70076413 - HCC TIPS Lot 2 COMET update\03 WIP\Transport Model scope\Count Data\Data processing\1_Raw data\For Github\1 - Raw Data"

#Update to user's C: Drive location for programming if there is an issue using this directory (copy of github repository)
RAW_DATA_C_DIRECTORY = f"{PROGRAM_DIRECTORY}\\1 - Raw Data"


TEMPLATE_FOLDER_STRUCTURE_CENTRAL_DATA_DIRECTORY = r"\\uk.wspgroup.com\central data\Projects\70076xxx\70076413 - HCC TIPS Lot 2 COMET update\03 WIP\Transport Model scope\Count Data\Data processing\Skeleton Data Processing Directory - do not delete HM\Directory_Template"
###########



##### 1 - RAW DATA
#2019 classified raw data directories
ATC_2019_C_1_1 = f"{PROGRAM_DIRECTORY}\\1 - Raw Data\\ATC\\2019\\Classified\\1_One_Tab_ATC_15m\\1_3_Tables\\"
ATC_2019_C_1_2 = f"{PROGRAM_DIRECTORY}\\1 - Raw Data\\ATC\\2019\\Classified\\1_One_Tab_ATC_15m\\2_4+_Tables\\"

#2019 non-classified raw data directories
ATC_2019_NC_1 = f"{PROGRAM_DIRECTORY}\\1 - Raw Data\\ATC\\2019\\Non-Classified\\1_Single_Week_Hourly\\"
ATC_2019_NC_2 = f"{PROGRAM_DIRECTORY}\\1 - Raw Data\\ATC\\2019\\Non-Classified\\2_Weekly_Tab_Seperated_by_Hour\\"
ATC_2019_NC_3 = f"{PROGRAM_DIRECTORY}\\1 - Raw Data\\ATC\\2019\\Non-Classified\\3_One_Week_15m\\"

#2022 classified raw datadirectories
ATC_2022_C_1_1 = f"{PROGRAM_DIRECTORY}\\1 - Raw Data\\ATC\\2022\\Classified\\1_One_Tab_ATC_15m\\1_3_Tables\\"
ATC_2022_C_1_2 = f"{PROGRAM_DIRECTORY}\\1 - Raw Data\\ATC\\2022\\Classified\\1_One_Tab_ATC_15m\\2_4+_Tables\\"

ATC_2022_C_2_1 = f"{PROGRAM_DIRECTORY}\\1 - Raw Data\\ATC\\2022\\Classified\\2_Tab_Seperated_ByDay_Classified_ATCs_15m\\1_General\\"
ATC_2022_C_2_2 = f"{PROGRAM_DIRECTORY}\\1 - Raw Data\\ATC\\2022\\Classified\\2_Tab_Seperated_ByDay_Classified_ATCs_15m\\2_Edge_Case\\"
#2022 non-classified raw data directories
ATC_2022_NC_1 = f"{PROGRAM_DIRECTORY}\\1 - Raw Data\\ATC\\2022\\Non-Classified\\1_Single_Week_Hourly\\"
ATC_2022_NC_2 = f"{PROGRAM_DIRECTORY}\\1 - Raw Data\\ATC\\2022\\Non-Classified\\2_Weekly_Tab_Seperated_by_Hour\\"
ATC_2022_NC_3 = f"{PROGRAM_DIRECTORY}\\1 - Raw Data\ATC\\2022\\Non-Classified\\3_One_Week_15m\\"

#2023 Severnside raw data:
ATC_2023_SEVERNSIDE_RAW = f"{PROGRAM_DIRECTORY}\\1 - Raw Data\\ATC\\2023\\Severnside\\"

#2023 HCC raw data:
ATC_2023_HCC_NC_1 = f"{PROGRAM_DIRECTORY}\\1 - Raw Data\\ATC\\2023\\HCC_Sites\\Non-Classified\\1_Daily_Column_Seperated_By_Hour\\"
ATC_2023_HCC_NC_3 = f"{PROGRAM_DIRECTORY}\\1 - Raw Data\\ATC\\2023\\HCC_Sites\\Non-Classified\\3_One_Week_15m\\"

ATC_2023_HCC_C_1_1 = f"{PROGRAM_DIRECTORY}\\1 - Raw Data\\ATC\\2023\\HCC_Sites\\Classified\\1_15m_UC_Seperated_1_sheet\\1_3_Tables\\"
ATC_2023_HCC_C_1_2 = f"{PROGRAM_DIRECTORY}\\1 - Raw Data\\ATC\\2023\\HCC_Sites\\Classified\\1_15m_UC_Seperated_1_sheet\\2_4+Tables\\"
ATC_2023_HCC_C_2 = f"{PROGRAM_DIRECTORY}\\1 - Raw Data\\ATC\\2023\\HCC_Sites\\Classified\\2_Daily_Column_Seperated_15M_Non_ITS\\"
####END OF RAW DATA



#### 2 - Formatted database directories
ATC_2019_15M_DB = f"{PROGRAM_DIRECTORY}\\2 - Formatted_DBs\\2019\\15m"
ATC_2019_HOURLY_DB = f"{PROGRAM_DIRECTORY}\\2 - Formatted_DBs\\2019\\Hourly"

ATC_2022_15M_DB = f"{PROGRAM_DIRECTORY}\\2 - Formatted_DBs\\2022\\15m"
ATC_2022_HOURLY_DB = f"{PROGRAM_DIRECTORY}\\2 - Formatted_DBs\\2022\\Hourly"

ATC_2023_15M_DB = f"{PROGRAM_DIRECTORY}\\2 - Formatted_DBs\\2023\\15m"
ATC_2023_HOURLY_DB = f"{PROGRAM_DIRECTORY}\\2 - Formatted_DBs\\2023\\Hourly"

DISCARDED_DATA = f"{PROGRAM_DIRECTORY}\\Discarded_Data\\"

ATC_FORMATTED_DBS = {"ATC_2019_15M_DB": {"year": 2019, "interval": "15m", "directory": ATC_2019_15M_DB},
                     "ATC_2019_HOURLY_DB": {"year": 2019, "interval": "Hourly", "directory": ATC_2019_HOURLY_DB},
                     "ATC_2022_15M_DB": {"year": 2022, "interval": "15m", "directory": ATC_2022_15M_DB},
                     "ATC_2022_HOURLY_DB": {"year": 2022, "interval": "Hourly", "directory": ATC_2022_HOURLY_DB},
                     "ATC_2023_15M_DB": {"year": 2023, "interval": "15m", "directory": ATC_2023_15M_DB},
                     "ATC_2023_HOURLY_DB": {"year": 2023, "interval": "Hourly", "directory": ATC_2023_HOURLY_DB},}



#### 3 - APPENDED DATABASE DIRECTORY

ATC_APPENDED = f"{PROGRAM_DIRECTORY}\\3 - Appended_DBs"


#########Filtering to neutral dates
DATE_FILTER_MON_THURS_NEUTRAL = f"{PROGRAM_DIRECTORY}\\util\\Monday-Thursdays.csv"


ATC_MON_THURS_NEUTRAL = f"{PROGRAM_DIRECTORY}\\4 - Filtered_DBs\\1 - Neutral_Day_DBs"

##########
##########Combining 15m and hourly DBs
ATC_NEUTRAL_COMBINED = f"{PROGRAM_DIRECTORY}\\4 - Filtered_DBs\\1 - Neutral_Day_DBs\\1b - Neutral_Day_DBs_Converted&Appended_to_Hourly"

##########Outlier filtering
ATC_OUTLIER_FILTERED = f"{PROGRAM_DIRECTORY}\\4 - Filtered_DBs\\2 - Outlier_Filtered_DBs"

ATC_COMBINED_OUTLIER_FILTERED = f"{PROGRAM_DIRECTORY}\\4 - Filtered_DBs\\2 - Outlier_Filtered_DBs\\2b - Outlier_Filtered_DBs_Converted&Appended_to_Hourly"



#Sample size filters for peak hour
SAMPLE_FILTER_8 = f"{PROGRAM_DIRECTORY}\\util\\2023_Peak_Hour_Sites_8+_Days.csv"
SAMPLE_FILTER_6 = f"{PROGRAM_DIRECTORY}\\util\\2023_Peak_Hour_Sites_6+_Days.csv"

PRE_PEAK_HOUR_DBS = f"{PROGRAM_DIRECTORY}\\4 - Filtered_DBs\\3 - Filtered_Peak_Hour_DBs_by_SampleSize"

PEAK_HOUR_INPUT_2023_6DAYS = f"{PRE_PEAK_HOUR_DBS}\\2023\\ATC_2023_15m_appended_neutral_mon-thurs_filtered_outliers_database_6.csv"

PEAK_HOUR_INPUT_2023_8DAYS = f"{PRE_PEAK_HOUR_DBS}\\2023\\ATC_2023_15m_appended_neutral_mon-thurs_filtered_outliers_database_8.csv"

PEAK_HOUR_2022 = f"{PROGRAM_DIRECTORY}\\Peak Hour Analysis\\2022"

PEAK_HOUR_2023 = f"{PROGRAM_DIRECTORY}\\Peak Hour Analysis\\2023"

A10_VALIDATION_SITE_FILTER = f"{PROGRAM_DIRECTORY}\\util\\A10_validation_sites.csv"

A10_VALIDATION_FULL_DB_DIRECTORY = f"{PROGRAM_DIRECTORY}\\A10_validation\\1_filtered_DBs\\"

A10_VALIDATION_AVERAGE_FLOW_DIRECTORY = f"{PROGRAM_DIRECTORY}\\A10_validation\\2_Average_Flows\\"


####### Filters, lookups, uplift factors etc
ATC_ROADTYPE_LOOKUP = f"{PROGRAM_DIRECTORY}\\util\\Site_RoadType_lookup.csv"

MONTHLY_VARIATION_FACTORS = f"{PROGRAM_DIRECTORY}\\util\\monthly_variation_factors\\"


###############