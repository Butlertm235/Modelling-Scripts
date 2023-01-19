"""
INSERT DESCRIPTION HERE

Author: Cesca Brint

Contributors:
"""

import numpy as np
import pandas as pd

def housing_summary_data(district_name):
    """
    INSERT FUNCTION DESCRIPTION
    """
    try:
        R7 = pd.read_csv(f'{district_name}_housing_data.csv')
    except:
        raise ValueError("Failed to load housing_data.csv file. File does not exist or is of the wrong structure")

    R7_summary = R7.drop(['Reference', 'Category'], axis=1)
    R7_summary = R7_summary.fillna('Empty')

    R7_summary.set_index(['Point Zone', 'Final COMET Zone'], inplace=True)

    R7_summary_sum = R7_summary.groupby(by=['Point Zone', 'Final COMET Zone']).agg(sum)
    R7_summary_sum = R7_summary_sum.rename(columns={'Total Dwellings': 'Sum Dwellings'})

    grand_total = R7_summary_sum['Sum Dwellings'].sum()
    windfall = float(R7_summary_sum['Sum Dwellings'].loc['Windfall'])

    R7_summary_sum['Windfall %'] = np.where(R7_summary_sum.index.get_level_values('Point Zone') == 'Windfall', 0, R7_summary_sum['Sum Dwellings'] / (grand_total - windfall))
    R7_summary_sum['Windfall Trips'] = np.where(R7_summary_sum.index.get_level_values('Point Zone') == 'Windfall', 0, R7_summary_sum['Windfall %']*windfall)
    R7_summary_sum['Total'] = np.where(R7_summary_sum.index.get_level_values('Point Zone') == 'Empty', R7_summary_sum['Sum Dwellings'] + R7_summary_sum['Windfall Trips'], R7_summary_sum['Sum Dwellings'])

    R7_summary_sum = R7_summary_sum.reset_index()
    R7_summary_sum.to_csv(f'{district_name}_housing_summary_data.csv', index=False)

def employment_summary_data(district_name):
    """
    INSERT FUNCTION DESCRIPTION
    """
    try:
        R7 = pd.read_csv(f'{district_name}_employment_data.csv')
    except:
        raise ValueError("Failed to load employment_data.csv file. File does not exist or is of the wrong structure")

    R7_summary = R7.drop(['Reference', 'Category', 'Total Floorspace_sqm'], axis=1)
    R7_summary = R7_summary.fillna('Empty')

    R7_summary.set_index(['Point Zone', 'Final COMET Zone'], inplace=True)

    R7_summary_sum = R7_summary.groupby(by=['Point Zone', 'Final COMET Zone']).agg(sum)
    R7_summary_sum = R7_summary_sum.rename(columns={'Total Jobs': 'Sum Jobs'})

    grand_total = R7_summary_sum['Sum Jobs'].sum()
    R7_summary_sum = R7_summary_sum.reset_index()

    R7_summary_sum.to_csv(f'{district_name}_employment_summary_data', index=False)
