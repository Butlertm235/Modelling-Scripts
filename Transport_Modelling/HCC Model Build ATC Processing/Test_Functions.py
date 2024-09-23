import pandas as pd
import File_directories
from Create_DBs import format_ATC_2019_NC_3


format_ATC_2019_NC_3(File_directories.ATC_2019_NC_3, File_directories.ATC_2019_HOURLY_DB)


#format_ATC_2022_C_1_2(File_directories.ATC_2022_C_1_2, File_directories.ATC_2022_15M_DB,File_directories.DISCARDED_DATA)

