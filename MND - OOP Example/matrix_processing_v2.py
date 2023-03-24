'''
import and process mobile network data from "raw" format into something usable
by a transport modelling program.
    
flowchart (Norwich matrix build process):
    Import raw MND
    Disaggregate MND to Model zone system
    Road:
        Import bus Synthetic;
        Convert bus Synthetic to proportion matrices
        Convert bus synthetic to same purposes as MND [deal with IB/OB diffs?];
        Take bus off MND road matrix;
        Convert Car+LGV person trip matrix to vehicle trips;
        Split purposes (HBO -> HBEmp, HBO);
        Split Car+LGV -> Car, LGV:
            *Determine total proportion of Car / LGV;
            *Determine purpose splits for each of Car / LGV;
            *Determine total number of Car / LGV for each purpose from above;
            Get TLD for LGVs to determine proportion of LGVs in each band;
            Move LGV trips to their own matrix.
        Synth combine (for short trips)
        Merge Inbound / Outbound trips;
        Add redistributed trips from Rail to Car matrices;
        Export.
    Rail:
        Split purposes (HBO -> HBEmp, HBO);
        Rail redistribution process:
            *Determine how rail stations are going to be redistributed;
            *Create lookup for rail station redistribution;
            Split outbound trips with >1.3km access (origin) 
                leg into their own matrix;
            Split inbound trips with >1.3km egress (destination) 
                leg into their own matrix;
        Synth combine (for short trips)
        Merge Inbound / Outbound trips;
        export.
'''
import pandas
import operator
from pathlib import Path, PurePath
import mnd_import_v2
from matrix_class_v2 import Zones, MatrixStack, Matrix, load_matrix
from matrix_decorators import standard_tools

MND_TIMES = {'AM': 'AM_peak_hr',
             'IP': 'Inter_peak',
             'PM': 'PM_peak_hr'}

WORKING_FOLDER = Path(r'C:\Users\UKTMB001\Documents\Python Scripts\MND')
MND_RAW_FOLDER = WORKING_FOLDER / '20200220 - Deliverable 1 Matrices'
INPUTS_FOLDER = WORKING_FOLDER / 'Inputs'
TEST_FOLDER = WORKING_FOLDER / 'Test'
OUTPUTS_FOLDER = WORKING_FOLDER / 'Outputs'
FINALISE_OUTPUTS_FOLDER = WORKING_FOLDER / 'Finalising Outputs' / 'Initial Outputs'

MND_ZONES_FILE = WORKING_FOLDER / 'ZoningTemplate_MSOA_script.xlsx'
MODEL_ZONES_FILE = WORKING_FOLDER / 'ZoningTemplate_Model_v2.xlsx'

#Iterates through given time periods loading each corresponding file
#SYNTH_BUS_FILES = {time_period: INPUTS_FOLDER / f'synth_bus_{time_period}.csv' 
#                   for time_period in MND_TIMES.values()}
SYNTH_BUS_FILES = {time_period: TEST_FOLDER / f'synth_bus_{time_period}.csv' 
                   for time_period in MND_TIMES.values()}
#SYNTH_CAR_FILES = {time_period: INPUTS_FOLDER / f'synth_car_{time_period}.csv' 
#                   for time_period in MND_TIMES.values()}
SYNTH_CAR_FILES = {time_period: TEST_FOLDER / f'synth_car_{time_period}_v2.csv' 
                   for time_period in MND_TIMES.values()}
#SYNTH_RAIL_FILES = {time_period: INPUTS_FOLDER / f'synth_rail_{time_period}.csv' 
#                   for time_period in MND_TIMES.values()}
SYNTH_RAIL_FILES = {time_period: TEST_FOLDER / f'synth_rail_{time_period}.csv' 
                   for time_period in MND_TIMES.values()}

PURPOSES_TEMPRO = INPUTS_FOLDER / 'Tempro_Data_V2.xlsx'
LGV_FILE = INPUTS_FOLDER / 'LGV_Info.xlsx'
RAIL_REDIST_FILE = INPUTS_FOLDER / 'Rail_Trips_Redistribution.xlsx'

EXTERNAL_FILTER_FILE = INPUTS_FOLDER / 'KLTM_WW_Zones_Rev7_OD_filter_matrix.csv'

URBAN_JOIN = 10000
RURAL_JOIN = 10000

MESSAGE_HOOK = print

#----------BASIC INPUTS----------
def get_time_period_factor(time_period) -> float:
    '''provides a time period factor for each time period in case it needs
    factoring
    '''
    time_period_values = {
        'AM': 1.0,
        'IP': float(1 / 6),
        'PM': 1.0}
    return time_period_values[time_period]

def get_pcu_factor(mode) -> float:
    '''provides a factor for modes in case they need factoring due to 
    passenger car unit values per vehicle
    '''
    factors = {
        'Car': 1.0,
        'LGV': 1.0,
        'Highway': 1.0,
        'HGV': 2.3}
    return factors[mode]

def get_disagg_purposes(mode_str: str,
                        disagg_modifier: str = "") -> dict:
    '''provides a mapping of origin and destination disaggregation datasets
    against their relevant purposes
    '''
    normal_purposes = {'IB_HBW': ('wppop', 'emp'),
            'OB_HBW': ('emp', 'wppop'),
            'IB_HBO': ('wppop', 'adults'),
            'OB_HBO': ('adults', 'wppop'),
            'NHBW': ('wppop', 'wppop'),
            'NHBO': ('wppop', 'wppop')}
    hgv_purposes = {'NHBW': ('wppop', 'wppop'),
            'NHBO': ('wppop', 'wppop')}
    disagg_purposes = {'Road': normal_purposes,
                       'Rail': normal_purposes,
                       'HGV': hgv_purposes}
    mode_disagg = disagg_purposes[mode_str]
    #Iterates through the disaggregated purposes and modifies the values depending on some modifier (STILL UNCLEAR WHAT THE VALUES AND MODIFIER WOULD MEAN) 
    mode_disagg_modified = {
        purpose: tuple(
            f"{value}{disagg_modifier}" for value in disagg_tuple)
        for purpose, disagg_tuple in mode_disagg.items()}
    return mode_disagg_modified

def get_rail_redist(redist_count: int = 2
                    ) -> 'pandas.DataFrame':
    '''imports a worksheet containing info on how to redistribute
    rail zones
    '''
    rail_redist_df = pandas.read_excel(RAIL_REDIST_FILE,
                                       sheet_name = 'Output')
    zone_dfs = []

    for number in range(1, redist_count + 1):
        #Iterates through each category of zone and then extracts the zone and factor data from them.
        #It then renames the categories and appends them together (possibly repeating the ID to have zone pairs? Bit unclear)
        rename = {'ID': 'from_zone', 
                   f'Zone {number}': 'to_zone', 
                   f'Factor {number}': 'proportion'}
        zone_df = rail_redist_df.loc[:, ['ID', 
                                         f'Zone {number}', 
                                         f'Factor {number}']]
        zone_df.rename(columns = rename,
                       inplace = True)
        zone_df.dropna(inplace = True)
        zone_dfs.append(zone_df)
    #Takes the first column to act as an ID that prefaces each zone pair and factor (Here is where it is unclear whether the zone pairs were always togehter
    # or if they are only just being put together. Surely popping the first column would break the first pair? To investigate with running and outputting)
    master_zone_df = zone_dfs.pop(0)
    for zone_df in zone_dfs:
        master_zone_df = pandas.concat([master_zone_df, zone_df],
                                       ignore_index=True)
    return master_zone_df

def get_synth_purposes() -> list:
    '''provides a list of the purposes that are contained in the synthetic
    '''
    return ['HBW', 'HBEmp', 'HBO', 'NHBW', 'NHBO']

def get_bus_purpose_map() -> dict:
    '''maps the bus purposes to the road purposes
    provided in case the purposes don't match up in the same order
    '''
    return  [('IB_HBW', 'IB_HBW'),
            ('OB_HBW', 'OB_HBW'),
            ('IB_HBO', 'IB_HBO'),
            ('OB_HBO', 'OB_HBO'),
            ('NHBW', 'NHBW'),
            ('NHBO', 'NHBO')]

def get_mnd_purposes(mode) -> list:
    '''returns a list of all of the purposes the MND contains in the raw data
    for the given mode
    '''
    return mnd_import_v2.MND_MODES[mode]

def get_mnd_directional_purposes() -> list:
    '''returns a list of those purposes that have inbound/outbound directions
    '''
    return ['HBW', 'HBO']

def get_ntem_map() -> dict:
    ntem_map = {'HBW': ['IB_HBW', 'OB_HBW'],
                'HBEmp': ['IB_HBEmp', 'OB_HBEmp'],
                'HBO': ['IB_HBO', 'OB_HBO'],
                'NHBW': ['NHBW'],
                'NHBO': ['NHBO']}
    return ntem_map

@standard_tools
def get_vehicle_occupancy(time_period: str) -> dict:
    '''calculates a set of vehicle occupancies for each purpose
    '''
    ## webtag values from May 2019 v1.12 table A1.3.3, under occupancy per trip
    #TODO update table of values
    #TODO update NHB values with a proportioned HBW/HBO?
    car_values = {'Work': {'AM': 1.20,
                            'IP': 1.19,
                            'PM': 1.17},
                  'Commuting': {'AM': 1.17,
                            'IP': 1.15,
                            'PM': 1.16},
                    'Other': {'AM': 1.68,
                            'IP': 1.65 ,
                            'PM': 1.71},
                    'Average': {'AM': 1.43,
                            'IP': 1.55,
                            'PM': 1.48}}
    lgv_value = 1.23
    trip_types = ['Work', 'Commuting', 'Other', 'Average']
    time_periods = MND_TIMES.keys()
    #Makes LGV values all the same for each time period and trip type?
    lgv_values = {trip_type: {time_p: lgv_value for 
                              time_p in time_periods}
                  for trip_type in trip_types}
    purpose_map = {'IB_HBW': 'Commuting',
                   'OB_HBW': 'Commuting',
                   'IB_HBO': 'Other',
                   'OB_HBO': 'Other',
                   'NHBW': 'Work',
                   'NHBO': 'Other'}
                   
    lgv_proportion = get_lgv_proportion(time_period)
    
    return_dict = {}
    #Scales occupancy value depending on the split between LGVs and Cars (Not sure why 1/ though, surely you don't want the inverse? Unless occupancy is meant to be less than 1?)
    for purpose, trip_type in purpose_map.items():
        return_dict[purpose] = 1 / (car_values[trip_type][time_period] * 
                                    (1 - lgv_proportion) + 
                                    lgv_values[trip_type][time_period] *
                                    lgv_proportion)
    return return_dict

@standard_tools
def get_purpose_proportions(time_period:str,
                            mode: str) -> 'pandas.DataFrame':
    '''gets a dictionary of purposes and converts to a hbemp proportion 
    (proportion based on hbemp vs all hbo)
    for each of origin and destination trips, using names as index
    '''
    purpose_file = PURPOSES_TEMPRO
    purpose_df = pandas.read_excel(
        purpose_file, 
        sheet_name = f"{time_period} - {mode}",
        header = [0,1],
        index_col = [0,1])
    hbo_df = purpose_df.loc[:, [column for column in purpose_df.columns
                                if (column[0] != 'HB Work' and
                                    column[0][:2] == 'HB')]]
    
    emp_str = 'Employers Business'
    prop_string = 'HBEmp_proportion'
    for direction in ['Origin', 'Destination']:
        new_col = (prop_string, direction)
        hbo_df[new_col] = (
            hbo_df.loc[:, [
                column for column in hbo_df.columns
                if (column[0][-1 * len(emp_str):] == emp_str and
                    column[1] == direction)]
                ].sum(axis=1) /
            hbo_df.loc[:, [
                column for column in hbo_df.columns
                if (column[1] == direction)]
                ].sum(axis=1))
    proportions_df = hbo_df.loc[:, [
        column for column in hbo_df.columns
        if column[0] == prop_string]]
    proportions_df.reset_index(0, drop=True,
                               inplace=True)
    proportions_df.set_index(proportions_df.index.str.replace('`', "'"),
                             inplace=True)
    return proportions_df

def get_lgv_proportion(time_period: str) -> float:
    '''LGV proportions taken from MCC data (see MCC Data.xlsx in inputs)
    '''
    #TODO update these values from MCC data
    lgv_proportions = {
        'AM': 0.139162158,
        'IP': 0.12614287,
        'PM': 0.098384324}
    return lgv_proportions[time_period]

def get_lgv_tld(time_period: str) -> dict:
    '''provides the trip length dist from Trafficmaster for LGVs
    '''
    tld_df = pandas.read_excel(LGV_FILE, sheet_name = 'tld',
                               header = [0,1], index_col = [0,1])
    tld_df = tld_df.loc[:, ('Proportions', time_period)]
    tld_dict = tld_df.to_dict()
    return tld_dict
    
def get_lgv_purpose_props(time_period: str,
                          ntem_map: dict) -> dict:
    '''provides a mapping of purpose to proportion of LGVs for that purpose
    based on controlling back to car purpose proportions from NTEM
    '''
    purpose_df = pandas.read_excel(LGV_FILE, sheet_name = 'purposes',
                                   header = [0], index_col = [0])
    purpose_df = purpose_df.loc[:, time_period]
    purpose_dict = purpose_df.to_dict()
    new_dict = {}
    for ntem_purpose, matrix_purposes in ntem_map.items():
        for purpose in matrix_purposes:
            new_dict[purpose] = purpose_dict[ntem_purpose]
    return new_dict

def get_final_purpose_dict(mode_str: str):
    highway_purposes = {'HBW': ['IB_HBW', 'OB_HBW'],
                            'HBEmp': ['IB_HBEmp', 'OB_HBEmp'],
                            'HBO': ['IB_HBO', 'OB_HBO'],
                            'NHBW': ['NHBW'],
                            'NHBO': ['NHBO'],
                            'LGV': ['LGV']}
    car_purposes = {'HBW': ['IB_HBW', 'OB_HBW'],
                            'HBEmp': ['IB_HBEmp', 'OB_HBEmp'],
                            'HBO': ['IB_HBO', 'OB_HBO'],
                            'NHBW': ['NHBW'],
                            'NHBO': ['NHBO']}
    pt_purposes = {'Commuting': ['IB_HBW', 'OB_HBW'],
                      'Employers Business': ['IB_HBEmp', 'OB_HBEmp', 'NHBW'],
                      'Other': ['IB_HBO', 'OB_HBO', 'NHBO']}
    pt_synth = {'Commuting': ['HBW'],
                      'Employers Business': ['HBEmp', 'NHBW'],
                      'Other': ['HBO', 'NHBO']}
    hgv_purposes = {'HGV': ['NHBW', 'NHBO']}
    purpose_dict = {'Highway': highway_purposes,
                    'Rail': pt_purposes,
                    'SynthRail': pt_synth,
                    'Car': car_purposes,
                    'Bus': pt_purposes,
                    'SynthBus': pt_synth,
                    'HGV': hgv_purposes}
    return purpose_dict[mode_str]

def get_combine_rules(urban_join: int = URBAN_JOIN,
                      rural_join: int = RURAL_JOIN):
    urban_distances = {(0, urban_join): 0,
                     (urban_join, 1000000000): 1}
    rural_distances = {(0, rural_join): 0,
                     (rural_join, 1000000000): 1}
    distances_dict = {1: urban_distances,
                      0: rural_distances}
    combine_rules = {'column_header': 'urbanrural',
                    'value_lookup': {'urban': 1,
                                     'rural': 0},
                    'value_type': 'urban_mask',
                    'result_type': 'mul',
                    'results': distances_dict}
    return combine_rules

def get_walk_distance():
    return 1333.3333
    
#----------IMPORTS------------

@standard_tools
def get_mnd_zones(zones_file_location: PurePath = MND_ZONES_FILE,
                  zones_sheet_name: str = 'zones') -> Zones:
    return mnd_import_v2.get_mnd_zones(zones_file_location,
                                    zones_sheet_name)

@standard_tools
def get_model_zones(zones_file_location: PurePath = MODEL_ZONES_FILE,
                    zones_sheet_name: str = 'zones') -> Zones:
    '''import a zones object for the model zone system
    '''
    zones = Zones(
        '2019_NWL_Model_zones',
        zones_file_location,
        zones_sheet_name)
    return zones

@standard_tools
def get_mnd_raw(time_period: str,
                vehicle_type: str,
                purposes: list = None,
                mnd_raw_folder: PurePath = MND_RAW_FOLDER,
                outputs_folder: PurePath = OUTPUTS_FOLDER,
                force_refresh: bool = False) -> dict:
    '''tries to load the data from the inputs folder, but if this doesn't
    exist will get the raw data instead
    data returned as {time_period: {vehicle_type: MatrixStack}}
    '''
    if not force_refresh:
        try:
            return mnd_import_v2.load_mnd(load_location = outputs_folder,
                                       time_period = time_period,
                                       vehicle_type = vehicle_type)
        except(FileNotFoundError):
            MESSAGE_HOOK(f"could not load from json files (using load_mnd), reverting to loading from raw data (file not found in: {outputs_folder} for {time_period}, {vehicle_type})")
            if purposes is None:
                raise ValueError(f"get_mnd_raw couldn't load, but needs a purpose list if loading from raw data")
    matrix_stack = mnd_import_v2.get_mnd_matrices(
        load_location = mnd_raw_folder,
        time_period = time_period,
        vehicle_type = vehicle_type,
        purpose_list = purposes,
        save_location = None)
    matrix_stack.save(outputs_folder)
    return matrix_stack
        
@standard_tools
def get_synth(synth_files: PurePath,
              vehicle_type: str,
              time_period: str,
              zone_system: Zones = None,
              output_folder: PurePath = OUTPUTS_FOLDER
              ) -> 'MatrixStack':
    '''imports synthetic matrices from a set of files
    '''
    time_period = MND_TIMES[time_period]
    if zone_system is None:
        zone_system = get_model_zones()

    synth_file = synth_files[time_period]
    matrices = MatrixStack(
        name = 'synth',
        zones = zone_system,
        time_period = time_period,
        vehicle_type = vehicle_type,
        value_type = 'Person Trips')
    level = 0
    for purpose in get_synth_purposes():
        level += 1
        matrix = Matrix(
            zones = zone_system,
            level = level,
            purpose = purpose,
            time_period = time_period,
            vehicle_type = vehicle_type,
            value_type = 'Person Trips')
        matrix.import_matrix_csv_square(
            matrix_file = synth_file,
            level_number = level)
        matrices.add_matrix(level, matrix)
    matrices.save(output_folder)
    return matrices

#-------PROCESSING-----
@standard_tools
def get_mnd_matrix_as_modelzone(time_period:str,
                                mode_str: str,
                                new_zone_system: 'Zones',
                                output_folder: 'PurePath',
                                force_refresh:bool = False
                                ) -> 'MatrixStack':
    '''gets a set of mnd matrices and disaggregates to model zone system
    '''
    name_str = f'mnd_modelzones'
    save_path = output_folder / f'{name_str}_{MND_TIMES[time_period]}_{mode_str}_Person Trips.json'
    if save_path.exists() and not force_refresh:
        MESSAGE_HOOK(f'get_mnd_matrix_as_modelzone: loading data from {save_path}')
        matrixstack = load_matrix(save_path)
    else:
        MESSAGE_HOOK(f'get_mnd_matrix_as_modelzone: generating data from scratch ({save_path} does not exist)')
        matrixstack = get_mnd_raw(MND_TIMES[time_period], 
                                  mode_str, 
                                  get_mnd_purposes(mode_str),
                                  force_refresh = force_refresh)
        matrixstack *= get_time_period_factor(time_period)
        disagg_purposes = get_disagg_purposes(mode_str)
        matrixstack = matrixstack.disaggregate(disagg_purposes, new_zone_system)
        matrixstack.name = name_str
        matrixstack.save(output_folder)
        matrixstack.save_user_classes_csv(output_folder, 'mnd_modelzones')
    return matrixstack

@standard_tools
def get_bus_proportions(road: 'MatrixStack',
                        bus: 'MatrixStack',
                        output_folder: 'PurePath',
                        mnd_directional_purposes: list,
                        bus_purpose_map: dict,
                        force_refresh: bool = False
                        ) -> 'MatrixStack':
    '''imports the bus matrices then converts them to 
    MND purposes (ib/ob splits) before converting to proportions
    based on the proportion of the total road matrix
    '''
    name_str = 'synth_mndpurpose'
    vehicle_type = 'Bus'
    prop_str = 'proportions'
    time_period = road.time_period
    save_path = output_folder / f'{name_str}_{time_period}_{vehicle_type}_{prop_str}.json'
    if save_path.exists() and not force_refresh:
        MESSAGE_HOOK(f'get_bus_proportions: loading data from {save_path}')
        bus_prop = load_matrix(save_path)
    else:
        MESSAGE_HOOK(f'get_bus_proportions: generating data from scratch ({save_path} does not exist)')
        ib_prop = {}
        bus_new = {}
        bus_mnd_purposes = MatrixStack(name = name_str,
                                       zones = bus.zones_object,
                                       time_period = bus.time_period,
                                       vehicle_type = bus.vehicle_type,
                                       value_type = bus.value_type)
        for purpose in mnd_directional_purposes:
            ib_sum = road.get_by_purpose(f'IB_{purpose}').matrix_sum
            ob_sum = road.get_by_purpose(f'OB_{purpose}').matrix_sum
            ib_prop[purpose] = ib_sum / (ib_sum + ob_sum)
            bus_new[f'OB_{purpose}'] = bus.get_by_purpose(purpose) * (1 - ib_prop[purpose])
            bus_new[f'OB_{purpose}'].purpose = f'OB_{purpose}'
            bus_new[f'IB_{purpose}'] = bus.get_by_purpose(purpose) * ib_prop[purpose]
            bus_new[f'IB_{purpose}'].purpose = f'IB_{purpose}'
        mnd_purposes = get_mnd_purposes('Road')
        for purpose in mnd_purposes:
            if purpose in bus.purposes:
                bus_mnd_purposes.add_matrix(
                    mnd_purposes.index(purpose) + 1,
                    bus.get_by_purpose(purpose))
            else:
                bus_mnd_purposes.add_matrix(
                    mnd_purposes.index(purpose) + 1,
                    bus_new[purpose])
        bus_prop = bus_mnd_purposes.proportions(
            other = road,
            mapping = bus_purpose_map,
            max_value = 1)
        bus_prop.save(output_folder)
        bus_prop.save_csv(output_folder, 'TEST')
    return bus_prop

@standard_tools
def convert_vehicle_occ(
        matrices: 'MatrixStack',
        purpose_factors: dict,
        output_folder: 'PurePath',
        force_refresh: bool = False
        ) -> 'MatrixStack':
    #TODO review whether it's worth using NTEM 
    #car occupancies instead of webtag?
    # suffolk process is signficantly more complicated but not sure how
    # much value it brings as it controls back to webtag factors
    # and ntem factors are much higher typically?
    save_path = output_folder / f'{matrices.name}_{matrices.time_period}_{matrices.vehicle_type}_Vehicle Trips.json'
    if save_path.exists() and not force_refresh:
        MESSAGE_HOOK(f'convert_vehicle_occ: loading data from {save_path}')
        new_matrices = load_matrix(save_path)
    else:
        MESSAGE_HOOK(f'convert_vehicle_occ: generating data from scratch ({save_path} does not exist)')
        new_matrices = matrices * purpose_factors
        new_matrices.value_type = 'Vehicle Trips'
        new_matrices.save(output_folder)
    return new_matrices

@standard_tools
def convert_purposes(
        matrices: 'MatrixStack',
        purpose_split_factors: 'pandas.DataFrame',
        output_folder: 'PurePath',
        force_refresh: bool = False
        ) -> 'MatrixStack':
    name_str = 'modelzones_splitpurpose'
    time_period = matrices.time_period
    vehicle_type = matrices.vehicle_type
    value_type = matrices.value_type
    save_path = output_folder / f'{name_str}_{time_period}_{vehicle_type}_{value_type}.json'
    if save_path.exists() and not force_refresh:
        MESSAGE_HOOK(f'convert_purposes: loading data from {save_path}')
        new_matrices = load_matrix(save_path)
    else:
        MESSAGE_HOOK(f'convert_purposes: generating data from scratch ({save_path} does not exist)')
        direction_data = {}
        for direction in ['Origin', 'Destination']:
            external_factor = purpose_split_factors.at[
                'GB', [column for column in purpose_split_factors.columns
                       if column[1] == direction][0]]
            purpose_dict = purpose_split_factors.loc[
                    [index for index in purpose_split_factors.index
                     if (len(index) > 3 and index[-3] == '0')], 
                    [column for column in purpose_split_factors.columns
                     if column[1] == direction]].to_dict()[
                             ('HBEmp_proportion', direction)]
            purpose_direction = (matrices.zones_object.
                create_info_list('msoa', purpose_dict, external_factor))
            direction_data[direction] = purpose_direction
        max_iterations = 100
        new_ob_hbo = matrices.get_by_purpose('OB_HBO').furness_matrix(
                    [1 - item for item in direction_data['Origin']], 
                    [1 - item for item in direction_data['Destination']],
                    max_iterations = max_iterations)
        new_ib_hbo = matrices.get_by_purpose('IB_HBO').furness_matrix(
                    [1 - item for item in direction_data['Origin']], 
                    [1 - item for item in direction_data['Destination']],
                    max_iterations = max_iterations)
        new_ob_hbemp = matrices.get_by_purpose('OB_HBO').furness_matrix(
                    direction_data['Origin'], 
                    direction_data['Destination'],
                    max_iterations = max_iterations)
        new_ob_hbemp.purpose = 'OB_HBEmp'
        new_ib_hbemp = matrices.get_by_purpose('IB_HBO').furness_matrix(
                    direction_data['Origin'], 
                    direction_data['Destination'],
                    max_iterations = max_iterations)
        new_ib_hbemp.purpose = 'IB_HBEmp'
        
        new_matrices = MatrixStack(
            name = name_str,
            zones = matrices.zones_object,
            time_period = matrices.time_period,
            vehicle_type = matrices.vehicle_type,
            value_type = matrices.value_type)
        new_matrices.add_matrix(1, matrices.get_by_purpose('IB_HBW'))
        new_matrices.add_matrix(2, matrices.get_by_purpose('OB_HBW'))
        new_matrices.add_matrix(3, new_ib_hbemp)
        new_matrices.add_matrix(4, new_ob_hbemp)
        new_matrices.add_matrix(5, new_ib_hbo)
        new_matrices.add_matrix(6, new_ob_hbo)
        new_matrices.add_matrix(7, matrices.get_by_purpose('NHBW'))
        new_matrices.add_matrix(8, matrices.get_by_purpose('NHBO'))
        new_matrices.save(output_folder)
    return new_matrices

@standard_tools
def lgv_split(
        matrices: 'MatrixStack',
        lgv_tld: dict,
        lgv_purpose_proportions: dict,
        output_folder: 'PurePath',
        force_refresh: bool = False
        ) -> 'MatrixStack':
    name_str = 'modelzones_splitlgv'
    time_period = matrices.time_period
    vehicle_type = 'Highway'
    save_path = output_folder / f'{name_str}_{time_period}_{vehicle_type}_Vehicle Trips.json'
    if save_path.exists() and not force_refresh:
        MESSAGE_HOOK(f'lgv_split: loading data from {save_path}')
        new_matrices = load_matrix(save_path)
    else:
        MESSAGE_HOOK(f'lgv_split: generating data from scratch ({save_path} does not exist)')
        distances = matrices.distances
        tld_bands = [band[0] for band in lgv_tld.keys()]
        new_matrices = MatrixStack(
            name = name_str,
            zones = matrices.zones_object,
            time_period = matrices.time_period,
            vehicle_type = vehicle_type,
            value_type = matrices.value_type)
        lgv_matrix = Matrix(matrices.zones_object,
                            purpose = 'LGV',
                            time_period = time_period,
                            vehicle_type = 'LGV',
                            value_type = 'Vehicle Trips'
                            )
        for purpose, proportion in lgv_purpose_proportions.items():
            matrix = matrices.get_by_purpose(purpose)
            lgv_total = matrix.matrix_sum * proportion
            matrix_tld = matrix.get_tld(tld_bands,
                                        distances = distances,
                                        intrazonals_bracket = False)
            lgv_totals = {band: lgv_total * lgv_tld_prop for
                          band, lgv_tld_prop in lgv_tld.items()}
            for band, lgv_total in lgv_totals.items():
                if lgv_total > (0.9 * matrix_tld[band]):
                    print(f'{lgv_total} greater than {matrix_tld[band]}: reducing to 90% of matrix value')
                    lgv_totals[band] = matrix_tld[band] * 0.9
            lgv_proportions = {}
            for band in lgv_totals.keys():
                if lgv_totals[band] > 0:
                    lgv_proportions[band] = lgv_totals[band] / matrix_tld[band]
                else:
                    lgv_proportions[band] = 0
            car_proportions = {band: 1 - lgv_prop for 
                               band, lgv_prop in lgv_proportions.items()}
            car = matrix.operate_by_property(car_proportions,
                                                    operator.mul,
                                                    distances)
            car.vehicle_type = 'car'
            new_matrices.add_matrix(matrix.level, car)
            lgv = matrix.operate_by_property(lgv_proportions,
                                                operator.mul,
                                                distances)
            lgv.vehicle_type = 'lgv'
            lgv_matrix = lgv_matrix + lgv
        
        
        #Ensure the lgv values reflect the observed lgv proprotion by scaling the car and lgv matricies accordingly
        if time_period == 'AM_peak_hr':
            time_period = 'AM'
        elif time_period == 'PM_peak_hr':
            time_period = 'PM'
        else:
            time_period = 'IP'
        target_proportion = get_lgv_proportion(time_period)

        lgv_sum = lgv_matrix.matrix_sum
        car_sum = sum(new_matrices.matrix_sums)
        total_sum = lgv_sum + car_sum

        #Set a target value for the LGV that can be used to find a scaling factor
        lgv_target = total_sum * target_proportion
        car_target = total_sum - lgv_target

        lgv_scaling_factor = lgv_target/lgv_sum
        car_scaling_factor = car_target/car_sum

        lgv_matrix *= lgv_scaling_factor
        new_matrices *= car_scaling_factor
        
        new_matrices.add_matrix(new_matrices.next_level, lgv_matrix)
        new_matrices.save(output_folder)
    return new_matrices

@standard_tools
def purpose_merge(
        matrices: 'MatrixStack',
        vehicle_type: str,
        purpose_dict: dict,
        output_folder: 'PurePath',
        force_refresh: bool = False
        ) -> 'MatrixStack':
    name_str = 'modelpurposes'
    time_period = matrices.time_period
    value_type = matrices.value_type
    save_path = output_folder / f'{name_str}_{time_period}_{vehicle_type}_{value_type}.json'
    if save_path.exists() and not force_refresh:
        MESSAGE_HOOK(f'purpose_merge: loading data from {save_path}')
        new_matrices = load_matrix(save_path)
    else:
        MESSAGE_HOOK(f'purpose_merge: generating data from scratch ({save_path} does not exist)')
        new_matrices = MatrixStack(
            name = name_str,
            zones = matrices.zones_object,
            time_period = matrices.time_period,
            vehicle_type = vehicle_type,
            value_type = matrices.value_type)
        for final_purpose, purpose_list in purpose_dict.items():
            final_purpose_matrix = None
            for current_purpose in purpose_list:
                if final_purpose_matrix is None:
                    final_purpose_matrix = matrices.get_by_purpose(
                        current_purpose)
                    final_purpose_matrix.purpose = final_purpose
                else:
                    final_purpose_matrix += matrices.get_by_purpose(
                        current_purpose)
                final_purpose_matrix.vehicle_type = vehicle_type
            new_matrices.add_matrix(new_matrices.next_level,
                                    final_purpose_matrix)
        new_matrices.save(output_folder)
    return new_matrices

@standard_tools
def rail_redistribution(
        matrices: 'MatrixStack',
        redist_df: 'pandas.DataFrame',
        walk_max: float,
        output_folder: 'PurePath',
        force_refresh: bool
        ) -> 'MatrixStack':
    '''takes the rail matrix and generates two new matrices:
        rail matrix where trip ends are moved if the trip to the station
        is more than the given walk distance;
        for those trips ends moved, a new trip is generated from that origin
        to the station, and these are saved to a new matrix for reassignment
        (assumed to be car trips in this instance)
    '''
    name_str = 'rail_redistribution'
    time_period = matrices.time_period
    rail_vehicle_type = 'Rail'
    car_vehicle_type = 'Car'
    rail_save_path = output_folder / f'{name_str}_{time_period}_{rail_vehicle_type}_Person Trips.json'
    car_redist_save_path = output_folder / f'{name_str}_{time_period}_{car_vehicle_type}_Vehicle Trips.json'
    if (rail_save_path.exists() and 
        car_redist_save_path.exists() and 
        not force_refresh):
        MESSAGE_HOOK(f'rail_redistribution: loading data from {rail_save_path}')
        rail_redist_matrices = load_matrix(rail_save_path)
        MESSAGE_HOOK(f'rail_redistribution: loading data from {car_redist_save_path}')
        car_redist_matrices = load_matrix(car_redist_save_path)
    else:
        MESSAGE_HOOK(f'rail_redistribution: generating data from scratch ({rail_save_path} or {car_redist_save_path} does not exist)')
        matrices_copy = matrices.copy()
        new_matrices = matrices_copy.add_disaggregation_map(
            'rail_redist',
            redist_df)
        purposes = new_matrices.purposes
        lookup = {}
        for purpose in purposes:
            if purpose[:2] == 'IB':
                lookup[purpose] = 'destination'
            else:
                lookup[purpose] = 'origin'
        rail_redist_matrices, car_redist_matrices = (
            new_matrices.redistribute('rail_redist',
                                      lookup,
                                      walk_max))
        rail_redist_matrices.name = name_str
        car_redist_matrices.name = name_str
        car_redist_matrices.vehicle_type = 'Car'
        car_redist_matrices.value_type = 'Vehicle Trips'
        
        rail_redist_matrices.save(output_folder)
        car_redist_matrices.save(output_folder)
        
    return rail_redist_matrices, car_redist_matrices

@standard_tools
def combine_mnd_synth(mnd_matrices: 'MatrixStack',
                      synth_matrices: 'MatrixStack',
                      rules: dict,
                      output_folder: 'PurePath',
                      force_refresh: bool = False,
                      synth_percentage = list):
    '''combines mobile network data with a synthetic dataset
    rules determines how this join happens
    rules should contain the inputs to create_matrix_from_column (Zones)
    as well as what this should do with the results. Also outputs
    the fraction of each matrix that is made from the synthetic matrices 
    '''
    name_str = 'combined_mndsynth'
    time_period = mnd_matrices.time_period
    value_type = mnd_matrices.value_type
    vehicle_type = mnd_matrices.vehicle_type
    print(f"Value Type: {value_type}\nVehicle Type: {vehicle_type}")
    if mnd_matrices.time_period != synth_matrices.time_period:
        raise ValueError(f"combine_mnd_synth given matrix stacks with different time periods ({mnd_matrices.time_period} and {synth_matrices.time_period})")
    save_path = output_folder / f'{name_str}_{time_period}_{vehicle_type}_{value_type}.json'
    if save_path.exists() and not force_refresh:
        MESSAGE_HOOK(f'combine_mnd_synth: loading data from {save_path}')
        combined = load_matrix(save_path)
    else:
        MESSAGE_HOOK(f'combine_mnd_synth: generating data from scratch ({save_path} does not exist)')
        rules_matrix = mnd_matrices.zones_object.create_matrix_from_column(
            column_header = rules['column_header'],
            value_type = rules['value_type'],
            result_type = rules['result_type'],
            value_lookup = rules['value_lookup'])
        distances = mnd_matrices.distances
        combined = None
        total_synth_sums = None
        total_time_period_synth = None
        time_period_synth_percentage = None
        print(f'mnd_start: {mnd_matrices.matrix_sums}')
        print(f'synth_start: {synth_matrices.matrix_sums}')
        for value, distance_dict in rules['results'].items():
            mnd_temp_matrices = mnd_matrices * (
                rules_matrix.matrix == value)
            mnd_for_join = mnd_temp_matrices.operate_by_property(
                distance_dict,
                operator.mul,
                distances)
            synth_temp_matrices = synth_matrices * (
                rules_matrix.matrix == value)
            print(synth_temp_matrices.matrix_sums)
            synth_for_join = synth_temp_matrices.operate_by_property(
                {band: 1 - band_value for band, band_value in
                 distance_dict.items()},
                operator.mul,
                distances)
            print(synth_for_join.matrix_sums)
            if combined is None:
                combined = mnd_for_join + synth_for_join

                #Find the total contribution from the synthetic matrices by user class and store in case combined is edited later
                total_synth_sums = synth_for_join.matrix_sums
                #For each user class matrix find the proportion made up from synthetic matrices 
                synth_percentage = [synth_value / combined_value for synth_value, combined_value in zip(total_synth_sums, combined.matrix_sums)]
                
                #Finds the total synthetic contribution for every user class
                total_time_period_synth = sum(total_synth_sums)
                time_period_synth_percentage = total_time_period_synth / sum(combined.matrix_sums)
                print(f"Percentage of matrices that is synthetic: {synth_percentage}")
            else:
                combined += mnd_for_join + synth_for_join
                
                #Update the total synthetic data added to enusre the percentage remains correct
                total_synth_sums = [new_synth_value + old_synth_total for new_synth_value, old_synth_total in zip(synth_for_join.matrix_sums, total_synth_sums)]
                synth_percentage = [total_synth_value / combined_value for total_synth_value, combined_value in zip(total_synth_sums, combined.matrix_sums)]
                
                total_time_period_synth = sum(total_synth_sums)
                time_period_synth_percentage = total_time_period_synth / sum(combined.matrix_sums)
                print(f"Percentage of matrices that is synthetic: {synth_percentage}")
            print(f'combined: {combined.matrix_sums}')
        print(f'final combined: {combined.matrix_sums}')
        combined.name = name_str
        combined.save(output_folder)

        synth_percentage.append(time_period_synth_percentage)
    return combined, synth_percentage

def get_sectored_matrices(matrices: 'MatrixStack',
                          output_folder: PurePath = OUTPUTS_FOLDER
                          ) -> 'MatrixStack':
    '''converts the matrix stack to a sectored version of the same stack
    '''
    sector_matrices = matrices.aggregate_by_zone_field('sector')
    sector_matrices.save(output_folder)
    sector_matrices.save_csv(output_folder)
    return sector_matrices

#-------------MAIN PROCESSES---------------

@standard_tools
def road_matrix_process(time_period: str,
                        output_folder: PurePath = OUTPUTS_FOLDER,
                        load_raw_data: bool = False,
                        force_refresh: bool = False
                        ) -> 'MatrixStack':
    '''
    Import MND and disaggregate to model zone system
    Import bus Synthetic;
    Convert bus Synthetic to proportion matrices
    Convert bus synthetic to same purposes as MND [deal with IB/OB diffs?];
    Split road into bus / non-bus;
    Split purposes (HBO -> HBEmp, HBO);
    Convert Car+LGV person trip matrix to vehicle trips;
    Split Car+LGV -> Car, LGV:
        *Determine total proportion of Car / LGV;
        *Determine purpose splits for each of Car / LGV;
        *Determine total number of Car / LGV for each purpose from above;
        Get TLD for LGVs to determine proportion of LGVs in each band;
        Move LGV trips to their own matrix.
    Synthetic merge for Car trips [no LGV/HGV synth matrix available];
    Merge Inbound / Outbound trips;
    '''
    road_str = 'Road'
    bus_str = 'Bus'
    car_str = 'Car'
    highway_str = 'Highway'
    synth_str = 'Synth'
        
    road = get_mnd_matrix_as_modelzone(
        time_period = time_period,
        mode_str = road_str,
        new_zone_system = get_model_zones(),
        output_folder = output_folder,
        force_refresh = load_raw_data)

    bus_synth = get_synth(SYNTH_BUS_FILES, bus_str, time_period)
    
    bus_prop = get_bus_proportions(road,
                        bus_synth,
                        output_folder,
                        get_mnd_directional_purposes(),
                        get_bus_purpose_map(),
                        force_refresh = force_refresh)
    
    bus_matrices = road * bus_prop
    bus_matrices.vehicle_type = bus_str
    bus_matrices.save(output_folder)
    bus_matrices.save_user_classes_csv(output_folder, 'mnd_bus')

    non_bus_matrices_person = road * bus_prop.invert()
    non_bus_matrices_person.vehicle_type = 'car+lgv'
    non_bus_matrices_person.save(output_folder)
    
    del road
    
    bus_splitpurp = convert_purposes(
        bus_matrices,
        get_purpose_proportions(time_period, bus_str),
        output_folder,
        force_refresh)
    
    bus_synth_modelpurpose = purpose_merge(
        bus_synth,
        f'{synth_str}{bus_str}',
        get_final_purpose_dict(f'{synth_str}{bus_str}'),
        output_folder,
        force_refresh)
    bus_modelpurp = purpose_merge(
        bus_splitpurp,
        bus_str,
        get_final_purpose_dict(bus_str),
        output_folder,
        force_refresh)
    
    del bus_synth
    del bus_splitpurp
    
    bus_synth_modelpurpose.save(output_folder)
    bus_synth_modelpurpose.save_csv(output_folder)
    bus_modelpurp.save(output_folder)
    bus_modelpurp.save_csv(output_folder)
    
    # TODO - review factor used for scaling trips in context of 
    # verification evidence
    bus_modelpurp *= 0.8
    bus_combined, bus_synth_percentage = combine_mnd_synth(
        bus_modelpurp,
        bus_synth_modelpurpose,
        get_combine_rules(),
        output_folder,
        force_refresh)
    #TODO - review factor used for scaling trips in context of 
    # verification evidence / model results
    bus_combined *= 0.7
    bus_combined.save(output_folder)
    bus_combined.save_csv(output_folder)
    
    del bus_matrices
    del bus_modelpurp
    del bus_synth_modelpurpose

    vehicle_proportions = get_vehicle_occupancy(time_period)
    non_bus_matrices = convert_vehicle_occ(non_bus_matrices_person,
                                           vehicle_proportions,
                                           output_folder,
                                           force_refresh)
    non_bus_matrices *= get_pcu_factor(highway_str)
    del vehicle_proportions
    del non_bus_matrices_person
    
    non_bus_matrices_splitpurp = convert_purposes(
        non_bus_matrices,
        get_purpose_proportions(time_period, car_str),
        output_folder,
        force_refresh)
    del non_bus_matrices
    
    non_bus_sums = non_bus_matrices_splitpurp.matrix_sums

    lgv_tld = get_lgv_tld(time_period)
    lgv_purpose_props = get_lgv_purpose_props(time_period,
                                              get_ntem_map())
    highway_matrices= lgv_split(non_bus_matrices_splitpurp,
                                    lgv_tld,
                                    lgv_purpose_props,
                                    output_folder,
                                    force_refresh)

    del non_bus_matrices_splitpurp

    highway_mnd_matrices = purpose_merge(
        highway_matrices,
        highway_str,
        get_final_purpose_dict(highway_str),
        output_folder,
        force_refresh)
    highway_mnd_matrices *= get_pcu_factor(highway_str)
    
    #TODO - review factor used for scaling trips in context of 
    # verification evidence
    highway_mnd_matrices *= 1.25
    highway_mnd_matrices.save_user_classes_csv(FINALISE_OUTPUTS_FOLDER, "TEST_MND")
    car_synth = get_synth(SYNTH_CAR_FILES,
                          car_str,
                          time_period)
    car_synth.save_user_classes_csv(FINALISE_OUTPUTS_FOLDER, "TEST_Synth")
    combined_highway_matrices, combined_highway_synth_percentage = combine_mnd_synth(
        highway_mnd_matrices,
        car_synth,
        get_combine_rules(),
        output_folder,
        force_refresh)

    #Ensures the lgv remains taken from the mnd as there is no synthetic matrix to combine it with
    combined_highway_matrices.add_matrix(6,highway_mnd_matrices.levels[6],True)

    del highway_mnd_matrices
    del car_synth
    print("Hi")
    return bus_combined, combined_highway_matrices, bus_synth_percentage, combined_highway_synth_percentage, non_bus_sums

def hgv_matrix_process(time_period: str,
                       output_folder: 'PurePath' = OUTPUTS_FOLDER,
                       load_raw_data:bool = False,
                       force_refresh: bool = False
                       ) -> 'MatrixStack':
    '''
    Import MND and disaggregate to model zone system;
    Merge HGV purposes (if there are purposes to merge);
    Scale by PCU factor if required;
    '''
    hgv_str = 'HGV'
    hgv_matrices = get_mnd_matrix_as_modelzone(
        time_period = time_period,
        mode_str = hgv_str,
        new_zone_system = get_model_zones(),
        output_folder = output_folder,
        force_refresh = load_raw_data)
    hgv_output_matrices = purpose_merge(
        hgv_matrices,
        hgv_str,
        get_final_purpose_dict(hgv_str),
        output_folder,
        force_refresh)
    hgv_output_matrices *= get_pcu_factor(hgv_str)
    hgv_output_matrices.value_type = 'Vehicle Trips'
    
    #TODO - review factor used for scaling trips in context of 
    # verification evidence and/or inrix data

    if time_period == "AM":
        hgv_output_matrices *= 2.75
    elif time_period == "IP":
        hgv_output_matrices *= 2.5
    elif time_period == "PM":
        hgv_output_matrices *= 1.9
    
    #hgv_output_matrices *= 2
    return hgv_output_matrices

TEST_FOLDER = Path(r'C:/Projects/temp')
def rail_matrix_process(time_period: str,
                        output_folder: 'PurePath' = OUTPUTS_FOLDER,
                        load_raw_data: bool = False,
                        force_refresh: bool = False
                        ) -> 'MatrixStack':
    '''
    Import MND and disaggregate to model zone system;
    Split purposes (HBO -> HBEmp, HBO);
    Rail redistribution process:
        *Determine how rail stations are going to be redistributed;
        *Create lookup for rail station redistribution;
        Split outbound trips with >1.3km access (origin) 
            leg into their own matrix;
        Split inbound trips with >1.3km egress (destination) 
            leg into their own matrix;
    Synthetic merge for rail trips;
    Merge Inbound / Outbound trips, aggregate Rail purposes to PT purposes;
    export.
    '''
    rail_str = 'Rail'
    car_str = 'Car'
    synth_str = 'Synth'
    walk_dist = get_walk_distance()

    rail_matrices = get_mnd_matrix_as_modelzone(
        time_period = time_period,
        mode_str = rail_str,
        new_zone_system = get_model_zones(),
        output_folder = output_folder,
        force_refresh = load_raw_data)
    
    purpose_proportions = get_purpose_proportions(time_period,
                                                  rail_str)
    rail_matrices_splitpurp = convert_purposes(rail_matrices,
                                        purpose_proportions,
                                        output_folder,
                                        force_refresh)
    
    redistribution = get_rail_redist()
    rail_redist_matrices, car_redist_matrices = rail_redistribution(
        rail_matrices_splitpurp,
        redistribution,
        walk_dist,
        output_folder,
        force_refresh)
        
    rail_synth = get_synth(SYNTH_RAIL_FILES,
                          rail_str,
                          time_period)
    
    rail_synth_modelpurpose = purpose_merge(
        rail_synth,
        f'{synth_str}{rail_str}',
        get_final_purpose_dict(f'{synth_str}{rail_str}'),
        output_folder,
        force_refresh)
    del rail_synth
    
    rail_modelpurposes = purpose_merge(
        rail_redist_matrices,
        rail_str,
        get_final_purpose_dict(rail_str),
        output_folder,
        force_refresh)
    rail_modelpurposes.save_csv(output_folder, 'OutputRailOnly')
    del rail_redist_matrices
    
    #TODO - review factor used for scaling trips in context of 
    # verification evidence / model data
    # rail_synth_modelpurpose *= 0.25
    # join distance increased to 20km based on trip length distribution
    # and distance between Acle and Norwich
    combined_rail_matrices, combined_rail_synth_percentage = combine_mnd_synth(
        rail_modelpurposes,
        rail_synth_modelpurpose,
        get_combine_rules(urban_join = 20000,
                          rural_join = 20000),
        output_folder,
        force_refresh)
    del rail_modelpurposes
    del rail_synth_modelpurpose
    
    car_redist_output_matrices = purpose_merge(
        car_redist_matrices,
        car_str,
        get_final_purpose_dict(car_str),
        output_folder,
        force_refresh)
    
    combined_rail_matrices.save(output_folder)
    combined_rail_matrices.save_csv(output_folder)
    
    return combined_rail_matrices, car_redist_output_matrices, combined_rail_synth_percentage
    
def highway_matrices_join(car_lgv_matrices: 'MatrixStack',
                          car_redist_trips: 'MatrixStack',
                          hgv_matrices: 'MatrixStack',
                          output_folder: 'PurePath' = OUTPUTS_FOLDER
                          ) -> 'MatrixStack':
    '''final tidying up for highway matrices
    aggregates the car/lgv/hgv matrices into a single MatrixStack
    after adding the car redistribution matrix
    '''
    highway_output_matrices = car_lgv_matrices.copy()
    highway_output_matrices += car_redist_trips
    for hgv_output_matrix in hgv_matrices.matrices:
        highway_output_matrices.add_matrix(
            highway_output_matrices.next_level,
            hgv_output_matrix)

    highway_output_matrices.name = 'Output'
    highway_output_matrices.save(output_folder)
    highway_output_matrices.save_csv(output_folder)
    return highway_output_matrices
    
def pt_mnd_only(time_period,
                output_folder: PurePath = OUTPUTS_FOLDER):
    name_str = 'modelpurposes'
    value_type = 'Person Trips'
    time_period = MND_TIMES[time_period]
    rail_path = output_folder / f'{name_str}_{time_period}_Rail_{value_type}.json'
    bus_path = output_folder / f'{name_str}_{time_period}_Bus_{value_type}.json'
    rail_matrices = load_matrix(rail_path)
    bus_matrices = load_matrix(bus_path)
    pt_matrices_join(rail_matrices, bus_matrices, name = 'OutputMNDOnly')

def pt_matrices_join(rail_matrices: 'MatrixStack',
                     bus_matrices: 'MatrixStack',
                     output_folder: 'PurePath' = OUTPUTS_FOLDER,
                     name: str = 'Output'
                     ) -> 'MatrixStack':
    '''final tidying up for public transport matrices
    sum together rail + bus to form overall matrix
    '''
    pt_matrices = rail_matrices + bus_matrices
    pt_matrices.name = name
    pt_matrices.vehicle_type = 'PublicTransport'
    
    pt_matrices.save(output_folder)
    pt_matrices.save_csv(output_folder)
    return pt_matrices

def pt_matrices_synth(time_period: str,
                      output_folder: 'PurePath' = OUTPUTS_FOLDER,
                      force_refresh: bool = False):
    rail_str = 'Rail'
    bus_str = 'Bus'
    synth_str = 'Synth'
    rail_synth = get_synth(SYNTH_RAIL_FILES,
                          rail_str,
                          time_period)
    rail_synth_modelpurpose = purpose_merge(
        rail_synth,
        f'{synth_str}{rail_str}',
        get_final_purpose_dict(f'{synth_str}{rail_str}'),
        output_folder,
        force_refresh)
    bus_synth = get_synth(SYNTH_BUS_FILES,
                          bus_str,
                          time_period)
    bus_synth_modelpurpose = purpose_merge(
        bus_synth,
        f'{synth_str}{bus_str}',
        get_final_purpose_dict(f'{synth_str}{bus_str}'),
        output_folder,
        force_refresh)
    pt_synth = rail_synth_modelpurpose + bus_synth_modelpurpose
    pt_synth.name = 'OutputSynth'
    pt_synth.vehicle_type = 'PublicTransport'
    
    pt_synth.save(output_folder)
    pt_synth.save_csv(output_folder)
    return pt_synth

def external_filter(target_matrix):
    """
    Applies a filter to a matrix to remove values that are in zones external to the model area.
    The filter matrix should be in a square format consisting of 1s and 0s.
    """
    
    MESSAGE_HOOK(f"Applying external filter to {target_matrix.file_name}")
    filter_matrix = Matrix(zones = get_model_zones(), value_type='Filter Factors')
    filter_matrix.import_matrix_csv_square(matrix_file=EXTERNAL_FILTER_FILE)
    filtered_matrix = target_matrix.operate_by_property({(0,1):0, (1,10):1}, operator.mul, filter_matrix)
    percentage_filtered = (sum(filtered_matrix.matrix_sums) / sum(target_matrix.matrix_sums)) * 100
    MESSAGE_HOOK(f"Matrix reduced to {percentage_filtered}%")

    return filtered_matrix

def output_synth_percentages(bus_percentage, car_percentage, rail_percentage, time_period):
    """
    Function to output the pecentage of a matrix that is made from the synthetic matrices
    """
    bus_output = pandas.DataFrame(columns=["Commuting", "Employer's Business", "Other", time_period])
    bus_percentage = pandas.Series(bus_percentage, index=["Commuting", "Employer's Business", "Other", time_period])
    bus_output = bus_output.append(bus_percentage, ignore_index=True)
    bus_output.to_csv(f"{OUTPUTS_FOLDER}/{time_period}_bus_synth_percentage.csv", index=False)

    rail_output = pandas.DataFrame(columns=["Commuting", "Employer's Business", "Other", time_period])
    rail_percentage = pandas.Series(rail_percentage, index=["Commuting", "Employer's Business", "Other", time_period])
    rail_output = rail_output.append(rail_percentage, ignore_index=True)
    rail_output.to_csv(f"{OUTPUTS_FOLDER}/{time_period}_rail_synth_percentage.csv", index=False)

    car_output = pandas.DataFrame(columns=["HBW", "HBEmp", "HBO", "NHBW", "NHBO", time_period])
    car_percentage = pandas.Series(car_percentage, index=["HBW", "HBEmp", "HBO", "NHBW", "NHBO", time_period])
    car_output = car_output.append(car_percentage, ignore_index=True)
    car_output.to_csv(f"{OUTPUTS_FOLDER}/{time_period}_car_synth_percentage.csv", index=False)

def output_non_bus_sums(non_bus_sums, time_period):
    non_bus_output = pandas.DataFrame(columns=["HBW_ib", "HBW_ob", "HBEmp_ib", "HBEmp_ob", "HBO_ib", "HBO_ob", "NHBW", "NHBO"])
    non_bus_sums = pandas.Series(non_bus_sums, index=["HBW_ib", "HBW_ob", "HBEmp_ib", "HBEmp_ob", "HBO_ib", "HBO_ob", "NHBW", "NHBO"])
    non_bus_output = non_bus_output.append(non_bus_sums, ignore_index=True)
    non_bus_output.to_csv(f"{OUTPUTS_FOLDER}/{time_period}_non_bus_sums.csv", index=False)

#-------TESTS---------

def test_imports():
    for time_period in MND_TIMES.keys():
        for mode, purposes in mnd_import_v2.MND_MODES.items():
            get_mnd_raw(time_period, mode, purposes)
    # get_synth_bus()
            

if __name__ == '__main__':
    pass
    # test_imports()
    for time_period in ['AM', 'IP', 'PM']:
    # for time_period in ['AM']:
        load_raw = True
        refresh = True
        bus, car_lgv, bus_synth_percentage, car_synth_percentage, non_bus_sums = road_matrix_process(time_period, 
                                            load_raw_data = load_raw,
                                            force_refresh = refresh)
        hgv = hgv_matrix_process(time_period, 
                                  load_raw_data = load_raw,
                                  force_refresh = refresh)
        rail, car_redist, rail_synth_percentage = rail_matrix_process(time_period, 
                                                load_raw_data = load_raw,
                                                force_refresh = refresh)
        highway = highway_matrices_join(car_lgv, car_redist, hgv)
        pt_mnd = pt_mnd_only(time_period)
        pt = pt_matrices_join(bus, rail)
        highway_sectored = get_sectored_matrices(highway)
        pt_sectored = get_sectored_matrices(pt)
        pt_synth = pt_matrices_synth(time_period,
                                      force_refresh = refresh)

        highway = external_filter(highway)
        pt = external_filter(pt)

        highway.save_user_classes_csv(FINALISE_OUTPUTS_FOLDER, "initial_output")
        highway_flat = highway.flatten(new_purpose = 'All_Flat', 
                                       new_vehicle_type = 'Highway')
        highway_flat.save_csv(OUTPUTS_FOLDER, 'Flat')
        highway_flat.save(OUTPUTS_FOLDER, 'Flat')
        highway_flat.output_summary(OUTPUTS_FOLDER, 'Flat_Summary')
        pt_flat = pt.flatten(new_purpose = 'All_Flat', 
                             new_vehicle_type = 'PT')
        pt_flat.save_csv(OUTPUTS_FOLDER, 'Flat')
        pt_flat.save(OUTPUTS_FOLDER, 'Flat')
        pt_flat.output_summary(OUTPUTS_FOLDER, 'Flat_Summary')
        output_synth_percentages(bus_synth_percentage, car_synth_percentage, rail_synth_percentage, time_period)
        output_non_bus_sums(non_bus_sums, time_period)