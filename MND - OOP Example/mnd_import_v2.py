from pathlib import Path, PurePath
from matrix_class_v2 import MatrixStack, Zones, Matrix, load_matrix
from matrix_decorators import standard_tools

PERSON_TRIPS = 'Person Trips'
MESSAGE_HOOK = print
CENTRAL_DATA_DIR = Path(r'C:\Users\UKTMB001\Documents\Python Scripts\MND')
SAVE_LOCATION = CENTRAL_DATA_DIR / 'Outputs'
LOCAL_DIR = Path(r'C:\Users\UKTMB001\Documents\Python Scripts\MND')
WORKING_FOLDER = LOCAL_DIR
LOAD_LOCATION = WORKING_FOLDER / '20200220 - Deliverable 1 Matrices'
SAVE_LOCATION = WORKING_FOLDER / 'Outputs'

MND_YEAR = 2019
TIME_PERIODS = ['AM_peak_hr', 'Inter_peak', 'PM_peak_hr']
TIME_PERIODS_ALL = ['AM_peak', 'AM_peak_hr', 'Inter_peak', 
                    'PM_peak', 'PM_peak_hr', 'Off_peak']
TIME_PERIODS_DAILY = ['AM_peak', 'Inter_peak', 'PM_peak', 'Off_peak']
MND_PURPOSES = ['IB_HBW', 'OB_HBW', 'IB_HBO', 'OB_HBO', 'NHBW', 'NHBO']
HGV_PURPOSES = ['NHBW', 'NHBO']
MND_MODES = {'Road': MND_PURPOSES,
             'Rail': MND_PURPOSES,
             'HGV': HGV_PURPOSES}

DISAGG_FILES = {'area': (WORKING_FOLDER / 'Inputs/New_Area.csv', 'New_Area'),
                'adults': (WORKING_FOLDER / 'Inputs/Total_Adults.csv', 'SUM_Adlt_P'),
                'emp': (WORKING_FOLDER / 'Inputs/Total_Employed_People.csv', 'SUM_Emp_Pe'),
                'wppop': (WORKING_FOLDER / 'Inputs/Total_WorkPlace_Pop.csv', 'SUM_Pop_Pr')}

@standard_tools
def get_mnd_zones(zones_file_location: PurePath = None,
                  zones_sheet_name: str = 'zones',
                  disagg_files: dict = DISAGG_FILES,
                  disagg_from: str = 'Zone_MSOA_',
                  disagg_to: str = 'Zone_Id'):
    if zones_file_location is None:
        zones_file_location = WORKING_FOLDER / 'ZoningTemplate_MSOA_script.xlsx'
    zones = Zones('MND_Raw_Zones', 
                  zones_file_location,
                  zones_sheet_name)
    for name, info in disagg_files.items():
        zones.import_disaggregation_map(name,
                                        info[0],
                                        from_zone = disagg_from,
                                        to_zone = disagg_to,
                                        value = info[1])
    return zones

@standard_tools
def load_mnd(time_period: str,
             vehicle_type: str,
             load_location: PurePath = LOAD_LOCATION,
             force_load: bool = False) -> MatrixStack:
    stack_path = load_location / f'MND_Raw_{time_period}_{vehicle_type}_{PERSON_TRIPS}.json'
    if not stack_path.exists():
        raise FileNotFoundError(f"load_mnd: File not found {stack_path}")
    return load_matrix(stack_path)

@standard_tools
def get_mnd_matrices(time_period: str,
                     vehicle_type: str,
                     purpose_list: list,
                     load_location: PurePath = LOAD_LOCATION,
                     save_location: PurePath = SAVE_LOCATION,
                     zones: 'Zones' = None) -> dict:
    """Imports raw data files
    returns a dictionary with time periods as keys
        and MatrixStack objects as items
    """
    if zones == None:
        zones = get_mnd_zones()
    queries = [f'period == "{time_period}"']
    matrix_stack = MatrixStack('MND_Raw',
                               zones = zones,
                               time_period = time_period,
                               vehicle_type = vehicle_type,
                               value_type = PERSON_TRIPS)
    level = 0
    for purpose in purpose_list:
        level += 1
        matrix = get_mnd_matrix(
            zones = zones,
            mnd_data_dir = load_location,
            level = level,
            purpose = purpose,
            time_period = time_period,
            vehicle_type = vehicle_type,
            queries = queries,
            save_location = save_location)
        matrix_stack.add_matrix(level, matrix)
        MESSAGE_HOOK(f'imported {time_period} {vehicle_type} {purpose} matrix')
    return matrix_stack

@standard_tools
def get_mnd_matrix(zones: 'Zones',
                   mnd_data_dir: PurePath,
                   level: int,
                   purpose: str,
                   time_period: str,
                   vehicle_type: str,
                   queries: list = None,
                   save_location: PurePath = None):
    matrix_file = mnd_data_dir / f'D1_Weekday_{purpose}_{vehicle_type}.csv'
    if not matrix_file.exists():
        raise FileNotFoundError(f"get_mnd_matrix: File not found {matrix_file}")
    matrix = Matrix(zones, 
                    level = level, 
                    purpose = purpose, 
                    time_period = time_period, 
                    vehicle_type = vehicle_type, 
                    value_type = 'Person Trips',
                    comments='MND Raw Data')
    matrix.import_matrix(mnd_data_dir / matrix_file,
                         sheet_name = 'record',
                         origin_field = 'start_zone',
                         destination_field = 'end_zone',
                         values_field = 'avg_daily_trips',
                         queries = queries,
                         output_workings_folder = save_location)
    return matrix
    
@standard_tools
def get_all_mnd(load_location: PurePath = LOAD_LOCATION,
                time_periods: list = TIME_PERIODS_ALL
                ) -> dict:
    matrices = {}
    zones = get_mnd_zones()
    for time_period in time_periods:
        matrices[time_period] = {}
        for vehicle_type, purposes in MND_MODES.items():
            matrix_stack = get_mnd_matrices(time_period,
                                            vehicle_type,
                                            purposes,
                                            load_location = load_location,
                                            save_location = None,
                                            zones = zones)
            matrices[time_period][vehicle_type] = matrix_stack
            matrix_stack.save(SAVE_LOCATION)
    return matrices

@standard_tools
def get_daily_jtw_mnd(load_location: PurePath = LOAD_LOCATION):
    purposes = ['OB_HBW']
    time_periods = ['AM_peak', 'Inter_peak', 'PM_peak', 'Off_peak']
    modes = ['Road', 'Rail']
    zones = get_mnd_zones()
    matrices = {}
    for mode in modes:
        matrices[mode] = {}
        for time_period in time_periods:
            matrix_stack = get_mnd_matrices(time_period,
                                            mode,
                                            purposes,
                                            load_location = load_location,
                                            save_location = None,
                                            zones = zones)
            matrices[mode][time_period] = matrix_stack.flatten(
                new_level = 1,
                new_purpose = 'JTW',
                new_vehicle_type = mode)
        daily_matrix = None
        for matrix in matrices[mode].values():
            if daily_matrix is None:
                daily_matrix = matrix
            else:
                daily_matrix += matrix
        matrices[mode]['Daily'] = daily_matrix
        matrices[mode]['Daily'].time_period = 'Daily'
        matrices[mode]['Daily'].save(SAVE_LOCATION, mode)
        matrices[mode]['Daily'].output_summary(SAVE_LOCATION, mode)
        matrices[mode]['Daily'].save_csv(SAVE_LOCATION, mode)
    return matrices


@standard_tools
def get_daily(load_location: PurePath = LOAD_LOCATION):
    purposes = MND_PURPOSES
    time_periods = ['AM_peak', 'Inter_peak', 'PM_peak', 'Off_peak']
    modes = ['Road', 'Rail']
    zones = get_mnd_zones()
    matrices = {}
    for mode in modes:
        matrices[mode] = {}
        for time_period in time_periods:
            matrix_stack = get_mnd_matrices(time_period,
                                            mode,
                                            purposes,
                                            load_location = load_location,
                                            save_location = None,
                                            zones = zones)
            matrices[mode][time_period] = matrix_stack
        daily_matrices = MatrixStack(
            matrix_stack.name,
            zones,
            'Daily',
            mode,
            'Person Trips')
        for time_period, matrix_stack in matrices[mode].items():
            if daily_matrices.levels == {}:
                for level, matrix in matrix_stack.levels.items():
                    matrix = matrix.copy()
                    matrix.time_period = 'Daily'
                    daily_matrices.add_matrix(level, matrix)
            else:
                daily_matrices += matrix_stack
        matrices[mode]['Daily'] = daily_matrices
        matrices[mode]['Daily'].save(SAVE_LOCATION, mode)
        matrices[mode]['Daily'].save_csv(SAVE_LOCATION, mode)
    return matrices


@standard_tools
def get_mnd(load_location: PurePath = LOAD_LOCATION,
            time_periods: list = TIME_PERIODS):
    return get_all_mnd(load_location, time_periods)

@standard_tools
def load_all_mnd(load_location: PurePath = SAVE_LOCATION,
                 time_periods: list = TIME_PERIODS_ALL):
    matrices = {}
    for time_period in time_periods:
        matrices[time_period] = {}
        for vehicle_type, purposes in MND_MODES.items():
            stack_path = load_location / f'MND_Raw_{time_period}_{vehicle_type}_{PERSON_TRIPS}.json'
            matrices[time_period][vehicle_type] = load_matrix(stack_path)
    return matrices

@standard_tools
def all_flat_csv(load_location: PurePath = LOAD_LOCATION,
                 save_location: PurePath = SAVE_LOCATION):
    matrices = get_all_mnd(load_location)
    for time_period, vehicle_info in matrices.items():
        for vehicle_type, matrix_stack in vehicle_info.items():
            flat_matrix = matrix_stack.flatten(
                new_vehicle_type = vehicle_type)
            flat_matrix.save_csv(save_location, 'MND_Raw_flat')
            flat_matrix.output_summary(save_location, 'MND_Raw_flat')

@standard_tools
def get_mnd_tlds(matrices: dict = None,
                brackets: list = None):
    '''gets a dictionary of trip length distributions for the MND matrices
    matrices needs to be a {time periods: {vehicle types: MatrixStack}}
    brackets needs to be a list of those items you want to use as breaks
        for the tld. an easy way to do this is generate it with range
        e.g. list(range(0, 100000, 5000)) will generate a list between
        0 and 100000 in 5000 intervals. distances are generated in metres.
    '''
    if matrices == None:
        matrices = load_all_mnd()
    if brackets == None:
        brackets = list(range(0, 100000, 5000))
    tlds = {}
    for time_period, vehicle_types in matrices.items():
        tlds[time_period] = {}
        for vehicle_type, matrix_stack in vehicle_types.items():
            tlds[time_period][vehicle_type] = {}
            for matrix in matrix_stack.levels.values():
                matrix_tld = matrix.get_tld(brackets)
                tlds[time_period][vehicle_type][matrix.purpose] = matrix_tld
    return tlds
    

if __name__ == '__main__':
    pass
    matrix_stack = get_mnd_matrices('AM_peak_hr',
                      'road',
                      MND_PURPOSES,
                      save_location = None)
    prefix = 'MND_Raw'
    for matrix in matrix_stack.levels.values():
         matrix.output_summary(SAVE_LOCATION,
                               prefix)
         print(matrix.save(SAVE_LOCATION, prefix))
    flat = matrix_stack.flatten(new_vehicle_type='road')
    prefix_flat = f'{prefix}_flat'
    flat.output_summary(SAVE_LOCATION,
                         prefix_flat)
    print(flat.save(SAVE_LOCATION, prefix_flat))
    all_flat_csv()
    get_all_mnd()
    get_mnd()
    load_all_mnd()
    print(get_mnd_tlds())
    get_daily_jtw_mnd()
    zones = get_mnd_zones()
    matrices = get_mnd_matrices('AM_peak_hr', 'Road', ['OB_HBW'])
    daily_matrices = get_daily()