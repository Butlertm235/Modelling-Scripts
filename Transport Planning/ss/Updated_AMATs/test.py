import pandas as pd
import openpyxl
import shutil
import os

excel_files = ['AMAT_Barton_section_1.xlsx','AMAT_Barton_section_2.xlsx','AMAT_Barton_section_3.xlsx','AMAT_Barton_section_4.xlsx','AMAT_Barton_section_5.xlsx','AMAT_Barton_section_6.xlsx','AMAT_Barton_section_7.xlsx','AMAT_Barton_section_8.xlsx','AMAT_Barton_section_9.xlsx']

from openpyxl import Workbook, load_workbook
for individual_excel_file in excel_files:
    AMAT = openpyxl.load_workbook(individual_excel_file)
    AMAT_ws = AMAT['User Interface Intervention']
    AMAT_ws['F83'].value = '=5.14'
    AMAT_ws['F110'].value = '=305'
    AMAT.save('Updated_AMATs/' + individual_excel_file)