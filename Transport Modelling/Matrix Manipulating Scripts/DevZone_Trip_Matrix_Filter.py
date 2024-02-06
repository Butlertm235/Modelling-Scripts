import pandas as pd
import os
import glob

"""
A program used to filter a matrix for rows and columns corresponding to a given list of zones (script produced for developer test).

All other entries will become zero.

This was used to output development trips from the SFM forecast matrix output to be added to the DM matrix. SFM saved here:

\\uk.wspgroup.com\central data\Projects\70091xxx\70091242 - Whitfield Danescroft\03 WIP\TP Transport Planning\01 Analysis & Calcs\VISUM\Aug_2023_Rerun\Scenario 2\SFM - AM Danescroft_DS2_2023.xlsm

Batch file used to output matrices to desired csv format saved here (file names within the .KEY files may need to be changed in constituent folders):

\\uk.wspgroup.com\central data\Projects\70091xxx\70091242 - Whitfield Danescroft\03 WIP\TP Transport Planning\01 Analysis & Calcs\Matrix Development\Reg_19_2023\Output_Matrices.bat

NOTE trip numbers for each of the development zones include trips from development being tested, and no background DM trips

The resulting matrices can be pasted into VISUM

TO DO:
- Input variables (block capitals)
    1. Change number of zones variable (252 used in the DDTM when script was developed)
    2. List UCs required in filtered trip matrix
    3. List zones to be filtered (Whitfield development spans across 737 and 871 so these are default)
    4. Change directory to that containing the matrices to be filtered, with matrices in .csv files
    5. Rename Output folder if necessary
    6. Run

    
Author: Hayden McCarthy
"""


#Input number of zones to use
NUMBER_OF_ZONES = 252

#Intput UCs to use
UCS_TO_USE = [1]

#Index Matrices Required
MATRIX_INDICES = [x - 1 for x in UCS_TO_USE]

#Zones to keep
ZONES_TO_KEEP = [737, 871]

#Set input and output directories
os.chdir(f"C:\\Users\\UKHRM004\\Documents\\Dover\\Danescroft 2023\\Matrix csv\\")
OUTPUT_FOLDER = f"Adjusted_Matrices"


#Loop through matrices in folder
matrix_csv_list = glob.glob("*.csv")
for file in matrix_csv_list:

    print("Reading ", file, ", zeroing non-development trips")

    #Initalise UC index
    UC = 1

    #Read csvs one UC at a time, setting zones as index
    for matrix in pd.read_csv(file, chunksize = NUMBER_OF_ZONES, header = None, index_col = 0):
        if UC in UCS_TO_USE:

            print(f"editing matrix {file}, UC {UC}")

            #Define list of zones to zero
            zones = matrix.index
            ZONES_TO_ZERO = [y for y in zones if y not in ZONES_TO_KEEP]


            #Rename columns to match zones
            column_conversion = {}
            for zone in range(NUMBER_OF_ZONES):
                column_conversion[zone+1] = zones[zone]
            matrix = matrix.rename(columns = column_conversion)
            
            #matrix.loc[~(( matrix.index.isin(ZONES_TO_KEEP)) | (~matrixZONES_TO_KEEP))] = 0 #| matrix[ZONES_TO_KEEP]

            #Define matrix to update
            updated_matrix = matrix

            #Set unneeded entries to zero
            updated_matrix.loc[~updated_matrix.index.isin(ZONES_TO_KEEP),ZONES_TO_ZERO] = 0

            #Output to csv
            updated_matrix.to_csv(f"Adjusted_Matrices\\{file[:-4]}_UC{UC}_development_matrix.csv")
        
        #Append UC index
        UC+=1

