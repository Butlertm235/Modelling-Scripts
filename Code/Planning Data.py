import numpy as np
import pandas as pd

def housing_data(district_name):

    try:
        R7 = pd.read_excel(f'{district_name}_Template.xlsx', sheet_name='Housing New')
        R6 = pd.read_excel(f'{district_name}_Template.xlsx', sheet_name='Housing Old')
    except:
        raise ValueError("Failed to load Template.xlsx file. File does not exist or is of the wrong structure")

    #set index
    R6.set_index(['Category', 'Reference'], inplace=True)
    R7.set_index(['Category', 'Reference'], inplace=True)

    #Filter by dwellings of size >= 300 and form a new data set df_size
    R7_size_filtered = R7.loc[R7['Total Dwellings'] >= 300]
    R6_size_filtered = R6.loc[R6['Total Dwellings'] >= 300]

    old_refs=[]
    cols = ['Category', 'Reference', 'Total Dwellings', 'Final COMET Zone', 'Point Zone']
    R6_old_zones = pd.DataFrame(columns=cols, index = range(len(old_refs)))
    R6_old_zones.set_index(['Category', 'Reference'], inplace=True)
    for ref in R6_size_filtered.index.get_level_values('Reference'):
        if ref not in R7_size_filtered.index.get_level_values('Reference').unique():
            old_refs.append(ref)
            R6_size_filtered.loc[R6_size_filtered.index.get_level_values('Reference') == ref, 'Total Dwellings'] = 0
            old_zones = R6_size_filtered[R6_size_filtered.index.get_level_values('Reference')==ref]
            R6_old_zones = R6_old_zones.append(old_zones)

    R6_size_filtered

    point_zones = []
    for zone in R6_size_filtered['Point Zone']:
        if type(zone) == str:
            continue
        else:
            point_zones.append(zone)
    for zone in R7_size_filtered['Point Zone']:
        if type(zone) == str:
            continue
        else:
            point_zones.append(zone)
    max_point_zone = max(point_zones)

    for ref in R7_size_filtered.index.get_level_values('Reference'):
        if ref not in R6_size_filtered.index.get_level_values('Reference').unique():
            if R7_size_filtered.index.get_level_values('Point Zone') == None:
                R7_size_filtered.loc[R7_size_filtered.index.get_level_values('Reference') == ref,'Point Zone'] = max_point_zone + 1
                max_point_zone += 1
            else:
                pass
    
    R7_size_filtered.drop(['Total Dwellings', 'Final COMET Zone'], axis=1)

    #update R7 with new point zones
    R7.update(R7_size_filtered)

    R7 = R7.append(R6_old_zones)
    R7 = R7.reset_index()
    R7.to_csv(f'{district_name}_housing_data.csv', index=False)

def employment_data(district_name):

    try:
        R7 = pd.read_excel(f'{district_name}_Template.xlsx', sheet_name='Employment New')
        R6 = pd.read_excel(f'{district_name}_Template.xlsx', sheet_name='Employment Old')
    except:
        raise ValueError("Failed to load Template.xlsx file. File does not exist or is of the wrong structure")

    #set index
    R6.set_index(['Category', 'Reference'], inplace=True)
    R7.set_index(['Category', 'Reference'], inplace=True)

    #Filter by dwellings of size >= 300 and form a new data set df_size
    R7_size_filtered = R7.loc[R7['Total Jobs'] >= 500]
    R6_size_filtered = R6.loc[R6['Total Jobs'] >= 500]

    for ref in R6_size_filtered.index.get_level_values('Reference'):
        if ref not in R7_size_filtered.index.get_level_values('Reference').unique():
            R6_size_filtered.loc[R6_size_filtered.index.get_level_values('Reference') == ref, 'Total Jobs'] = 0

    old_refs=[]
    cols = ['Category', 'Reference', 'Total Floorspace_sqm', 'Total Jobs', 'Final COMET Zone', 'Point Zone']
    R6_old_zones = pd.DataFrame(columns=cols, index = range(len(old_refs)))
    R6_old_zones.set_index(['Category', 'Reference'], inplace=True)
    for ref in R6_size_filtered.index.get_level_values('Reference'):
        if ref not in R7_size_filtered.index.get_level_values('Reference').unique():
            old_refs.append(ref)
            R6_size_filtered.loc[R6_size_filtered.index.get_level_values('Reference') == ref, 'Total Jobs'] = 0
            old_zones = R6_size_filtered[R6_size_filtered.index.get_level_values('Reference')==ref]
            R6_old_zones = R6_old_zones.append(old_zones)

    R6_size_filtered

    point_zones = []
    for zone in R6_size_filtered['Point Zone']:
        if type(zone) == str:
            continue
        else:
            point_zones.append(zone)
    for zone in R7_size_filtered['Point Zone']:
        if type(zone) == str:
            continue
        else:
            point_zones.append(zone)
    max_point_zone = max(point_zones)

    for ref in R7_size_filtered.index.get_level_values('Reference'):
        if ref not in R6_size_filtered.index.get_level_values('Reference').unique():
            if R7_size_filtered.index.get_level_values('Point Zone') == None:
                R7_size_filtered.loc[R7_size_filtered.index.get_level_values('Reference') == ref,'Point Zone'] = max_point_zone + 1
                max_point_zone += 1
            else:
                pass
    
    R7_size_filtered.drop(['Total Floorspace_sqm', 'Total Jobs', 'Final COMET Zone'], axis=1)

    #update R7 with new point zones
    R7.update(R7_size_filtered)

    R7 = R7.append(R6_old_zones)
    R7 = R7.reset_index()
    R7.to_csv(f'{district_name}_employment_data.csv', index=False)