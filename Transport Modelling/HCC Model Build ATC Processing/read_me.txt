########################
Author: Hayden McCarthy
########################

Supporting Staff:
Aakif Naem
Thomas Butler

SCRIPT DETAILS:


File_Directories.py:

Constains string and dictionary definitions detailing input and output directories of raw and processed data for each of the different formats of raw data provided.

Naming convention:

	Raw Data directiories:
		{type of data}_{year}_{continuous/non continuous}_{folder number 1}*when necessary*_{folder number 2}
		for example: 
		ATC_2019_NC_3 corresponds to the directory for the third format (folder 3) of non-continuous ATC data for 2019
		ATC_2022_C_2_2 corresponds to the directory for the second format (folder 2) within folder 2 of continuous ATC data for 2022
	
	

The variable naming convention is also used for the functions within Create_DBs.py which 

