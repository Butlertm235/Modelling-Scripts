import pandas as pd
import File_directories
from typing import List
import os
import glob

def append_db_groups(input_dir: str, output_dir: str, output_db_name: str):

    """
    This function will take a list of directories containing formatted DBs to be appended, and output to a specified directory 
    with a specified file name.
    Different combinations of formatted DB directories should be used for different purposes. i.e. 15m kept together to be 
    converted into hourly after outlier filtering.
    """

    DBs_to_append = []
    
    os.chdir(input_dir)
    files = glob.glob("*.csv")
    for file in files:

        print("Reading ", file, ", and adding to the list of appended DBs")

        file_df = pd.read_csv(file, skiprows = 0)
        print("DB List Type: ", type(DBs_to_append))
        DBs_to_append.insert(len(DBs_to_append), file_df)
        print(DBs_to_append)

    print("Total DBs appended = ", len(DBs_to_append))

    Appended_DB = pd.concat(DBs_to_append)

    Appended_DB.to_csv(f"{output_dir}\\{output_db_name}.csv")

