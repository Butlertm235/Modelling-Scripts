import pandas as pd
import File_directories
import Create_DBs
import Data_Processing



#append formatted DBs
for DB in File_directories.ATC_FORMATTED_DBS.items():
    dict = DB[1]
    print(dict)
    Data_Processing.append_db_groups(dict['directory'],File_directories.ATC_APPENDED,f"ATC_{dict['year']}_{dict['interval']}_appended")





