#A programme to separate out appended demand matrix into their respective user classes and give the user choice of output format

from cmath import nan
import numpy as np
import pandas as pd
import os
import ctypes

number_of_zones = 0
zone_list = set()
cwd = os.getcwd()
current_user_class = 1

def zone_finder(row, used_zones):
    #Finds how many zones there are by iterating through the matrix adding zones till it finds repeats
    global number_of_zones

    if row.iloc[0] in used_zones:
        pass
    else:
        used_zones.add(row.iloc[0])
        number_of_zones += 1

user_matrix_name = input("What is the demand matrix file name? (Don't include .csv)\n")
demand_matrix_name = user_matrix_name + '.csv'

#Loads file and throws error window if not loaded correctly 
try:
    data = pd.read_csv(rf"{cwd}/{demand_matrix_name}")
except:
    ctypes.windll.user32.MessageBoxW(0, u'File not found!\n\nPlease make sure the file is in the same location as this program and check the file name.', u"Error", 0)
    raise FileNotFoundError('File not found! Please make sure the file is in the same location as this program and check the file name.')

#Finds the user's preference for output format
user_output_format = nan
while user_output_format != '1' and user_output_format != '2':
    user_output_format = input("Would you like the user class matrices output as: \n1. Separate csv files or... \n2. Separate sheets in one Excel workbook?\n")
    if user_output_format != '1' and user_output_format != '2':
        print('\nInput not recognised please try again. Only type "1" or "2" in your response\n')

#Finds out if the user would like to name their user classes manually
name_user_classes = nan
while name_user_classes != 'y' and name_user_classes != 'n':
    name_user_classes = input('Would you like to name the user classes or leave them as default (UC1, UC2 etc.)?\nAnswer y or n\n')
    if name_user_classes != 'y' and name_user_classes != 'n':
        print('\nInput not recognised please try again. Only type "y" or "n" in your response\n')

#Allows the user to give names for the user classes
if name_user_classes == 'y':
    finished_naming = False
    user_class_names = []
    user_class_number = 1
    print("\nPlease Note: if you name more user classes than are in the data, then only the first names will be taken and if any user classes are not named\nthey will have the default names.\n")

    while not finished_naming:
        if user_class_number != 1:
            user_finished = input('\nDo you want to name another user class?\nAnswer y or n\n')
            if user_finished == 'n' or user_finished == 'no':
                break
        
        user_class_names.append(input(f"What would you like to call user class {user_class_number}?\n"))
        user_class_number += 1

#Finds out the number of zones in one user class so the data can be divided correctly
data.apply(zone_finder, axis=1, args=[zone_list])
top_row = data.head(1)
del(data)

if user_output_format == '1':
    #Divides the appended demand matrix into separate csv files

    #If there is an error in chunking the user classes throw an error to check the structure of the data
    try:
        for demand_matrix in pd.read_csv(rf'{cwd}/{demand_matrix_name}',chunksize = number_of_zones):
            #Separates the large matrix into square user class matricies

            #Adds the top row of the matrix to each smaller square matrix
            if current_user_class != 1:
                demand_matrix = top_row.append(demand_matrix)
            print(demand_matrix)

            if name_user_classes == 'y' and current_user_class - 1 < len(user_class_names):
                #Uses the user's class names if their are any left to use
                demand_matrix.to_csv(f"{cwd}/{user_matrix_name}_{user_class_names[current_user_class - 1]}.csv", index = False)   
            else:
                #Resorts to default names if there are none left from the user
                demand_matrix.to_csv(f"{cwd}/{user_matrix_name}_UC{current_user_class}.csv", index = False)
            
            current_user_class += 1
    except:
        ctypes.windll.user32.MessageBoxW(0, u'File could not be chunked successfully!\n\nCheck the structure of the demand matrix!', u"Error", 0)
        raise FileNotFoundError('File could not be chunked successfully!\n\nCheck the structure of the demand matrix!')


else:
    #Divides the appended demand matrix into separate sheets within one excel workbook

    #Creates excel file for the demand matrices to be saved to
    excel_file = pd.ExcelWriter(f"{cwd}/{user_matrix_name}_all_UC.xlsx", engine = 'xlsxwriter', options={})

    #If there is an error in chunking the user classes throw an error to check the structure of the data
    try:
        for demand_matrix in pd.read_csv(rf'{cwd}/{demand_matrix_name}',chunksize = number_of_zones):
            #Separates the large matrix into square user class matricies

            #Adds the top row of the matrix to each smaller square matrix
            if current_user_class != 1:
                demand_matrix = top_row.append(demand_matrix)
            print(demand_matrix)

            if name_user_classes == 'y' and current_user_class - 1 < len(user_class_names):
                #Uses the user's class names if their are any left to use
                demand_matrix.to_excel(excel_file, sheet_name=f"UC{current_user_class} - {user_class_names[current_user_class - 1]}", index = False)   
            else:
                #Resorts to default names if there are none left from the user
                demand_matrix.to_excel(excel_file, sheet_name=f"UC{current_user_class}", index = False)

            current_user_class += 1

        #Saves the completed excel file
        excel_file.close()
    except:
        ctypes.windll.user32.MessageBoxW(0, u'File could not be chunked successfully!\n\nCheck the structure of the demand matrix!', u"Error", 0)
        raise FileNotFoundError('File could not be chunked successfully!\n\nCheck the structure of the demand matrix!')
