"""
A program that expands a matrix given a new set of model zones that have to be included.

Author: Thomas Butler

Contributors:
"""
import numpy as np
import pandas as pd
import os

#INPUTS
TIME_PERIODS = ["AM_peak_hr", "Inter_peak", "PM_peak_hr"]
USER_CLASSES = ["1_HBW", "2_HBEmp", "3_HBO", "4_NHBW", "5_NHBO", "6_LGV", "7_HGV"]
PROGRAM_DIRECTORY = os.getcwd()
FILE_DIRECTORY = f"{PROGRAM_DIRECTORY}\Finalising Outputs\Initial Outputs"
INITIAL_NUMBER_OF_ZONES = 169
NEW_MODEL_ZONES = [6030, 6140, 6141, 6148, 6200, 6201, 6202, 6203, 6204, 6205, 6206, 6207, 6208, 6210, 6211, 6500, 6501, 6502, 6503, 6504, 6505, 6506]

#A dictionary that stores all of the new rows to be added 
#(used a dictionary as it can be easily appended onto the end of a dataframe, may need updating if append is removed from pandas)
new_rows = dict()

row_index = 0
#Runs through the list of model zones creating a new row that starts with the model zone name and has 0 for all the zone's values
for model_zone_index in range(len(NEW_MODEL_ZONES)):
    row = dict()
    for i in range(INITIAL_NUMBER_OF_ZONES):
        if i == 0:
            row[i] = NEW_MODEL_ZONES[model_zone_index]
        else:
            row[i] = 0

    new_rows[row_index] = row
    row_index += 1

#Reads all outputs from the matrix processesor, adds the new model zones and outputs the matrices at a user class level and in a stacked format
for time_period in TIME_PERIODS:
    matrices = dict()
    stacked_matrices = pd.DataFrame()
    #Reads in the data 1 matrix at a time, each of which refer to a different time period and user class
    for user_class in USER_CLASSES:
        matrix = pd.DataFrame()
        #Accounts for quirk in matrix processing output names
        if user_class != "7_HGV":
            matrix = pd.read_csv(f"{FILE_DIRECTORY}\initial_output_{time_period}_{user_class}_Highway_Vehicle Trips_values.csv", header=None)
        else:
            matrix = pd.read_csv(f"{FILE_DIRECTORY}\initial_output_{time_period}_{user_class}_HGV_Vehicle Trips_values.csv", header=None) 

        #Add the new rows to the current matrix
        for z in range(len(new_rows)):
            matrix = matrix.append(new_rows[z], ignore_index=True)

        #Insert new columns to ensure the matrix remains square (arbitarily high value of 1000000 is selected for the column names as to not clash with any existing names)
        for z in range(len(NEW_MODEL_ZONES)):
            matrix.insert((160+z), (1000000+z), 0)

        #Ensure all zones names are of the same data type for sorting
        matrix[0] = matrix[0].astype(int)
        matrix = matrix.set_index(0).sort_index()

        #Create the stacked output by adding each matrix to a the current stack of matricies
        stacked_matrices = pd.concat([stacked_matrices,matrix], axis=0)
        
        #Create zone labels for the columns so the individual user classes can be checked and entered into visualsiation programs
        matrix.columns = matrix.index
        matrix.to_csv(f"{PROGRAM_DIRECTORY}\Finalising Outputs\Final Outputs\{time_period}_{user_class}_Trips.csv")
    
    #Output matrix stack when all user classes in the time period have been added
    stacked_matrices.to_csv(f"{PROGRAM_DIRECTORY}\Finalising Outputs\Final Outputs\{time_period}_Trips.csv", header=False)