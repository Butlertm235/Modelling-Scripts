import pandas as pd

import openpyxl

#Sensitivity 1 = Halve cycling uplift
#Cycling_uplift = 1.25
#Half_cycling_uplift = 1.125

#from pathlib import Path

#files = [file for file in Path('C:\Users\UKGSP004\Documents\Python Scripts\Barton Greenway').rglob('*filename*.xlsx')]
#AMATs = [ pd.read_excel(file, sheet_name='Resume', header=1, usecols='A:I') for file in files]

#df1 = pd.concat(AMATs)
#print(df1)

#AMAT1 = openpyxl.load_workbook(filename='AMAT_Barton_section_1.xlsx')
#print(AMAT1.describe())
#AMAT1_UII = AMAT1.get_sheet_by_name('User Interface Intervention')
#AMAT1_UII['F34'] = AMAT1_UII['F33']*Half_cycling_uplift
#AMAT1.save('AMAT_1_sensitivity_1.xlsx')

import pandas as pd
import openpyxl

#insert AMAT names in directory
excel_files = ['AMAT_Barton_section_1.xlsx','AMAT_Barton_section_2.xlsx','AMAT_Barton_section_3.xlsx','AMAT_Barton_section_4.xlsx','AMAT_Barton_section_5.xlsx','AMAT_Barton_section_6.xlsx','AMAT_Barton_section_7.xlsx','AMAT_Barton_section_8.xlsx','AMAT_Barton_section_9.xlsx']

for individual_excel_file in excel_files:
    AMAT = openpyxl.load_workbook(individual_excel_file)
    AMAT_ws = AMAT['User Interface Intervention']
    AMAT_ws['F34'].value = '=ROUND(F33*1.2,0)'
    AMAT_ws['F47'].value = '=ROUND(F46*1.1,0)'
    AMAT.save('AMATs/Sensitivity_1/'+individual_excel_file)