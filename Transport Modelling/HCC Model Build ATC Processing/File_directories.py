import pandas as pd
import os

PROGRAM_DIRECTORY = os.getcwd()

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
ATC_2022_NC_2 = f"{PROGRAM_DIRECTORY}\\1 - Raw Data\\ATC\\2022\\Non-Classified\\2_Daily_Column_Seperated_by_Hour\\"
ATC_2022_NC_3 = f"{PROGRAM_DIRECTORY}\\1 - Raw Data\\ATC\\2022\\Non-Classified\\3_Weekly_Tab_Seperated_by_Hour\\"
ATC_2022_NC_4 = f"{PROGRAM_DIRECTORY}\\1 - Raw Data\ATC\\2022\\Non-Classified\\4_One_Week_15m\\"


#Formatted database directories


ATC_2019_15M_DB = f"{PROGRAM_DIRECTORY}\\2 - Formatted_DBs\\2019\\15m"
ATC_2019_HOURLY_DB = f"{PROGRAM_DIRECTORY}\\2 - Formatted_DBs\\2019\\Hourly"

ATC_2022_15M_DB = f"{PROGRAM_DIRECTORY}\\2 - Formatted_DBs\\2022\\15m"
ATC_2022_HOURLY_DB = f"{PROGRAM_DIRECTORY}\\2 - Formatted_DBs\\2022\\Hourly"

DISCARDED_DATA = f"{PROGRAM_DIRECTORY}\\Discarded_Data\\"

ATC_FORMATTED_DBS = {"ATC_2019_15M_DB": {"year": 2019, "interval": "15m", "directory": ATC_2019_15M_DB},
                     "ATC_2019_HOURLY_DB": {"year": 2019, "interval": "Hourly", "directory": ATC_2019_HOURLY_DB},
                     "ATC_2022_15M_DB": {"year": 2022, "interval": "15m", "directory": ATC_2022_15M_DB},
                     "ATC_2022_HOURLY_DB": {"year": 2022, "interval": "Hourly", "directory": ATC_2022_HOURLY_DB},}
#Appended database directories

ATC_APPENDED = f"{PROGRAM_DIRECTORY}\\3 - Appended_DBs"

