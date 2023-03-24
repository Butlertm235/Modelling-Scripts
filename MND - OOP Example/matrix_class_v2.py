"""
Contains a series of classes for containing and manipulating
matrices, specifically designed for use in transport modelling

MatrixStack is a stack of matrices, where each matrix represents some slice
of demand, e.g. different modes of travel or purposes of travel

Matrix is a single level matrix representing some type of data, e.g. trips or
distance data. 

Zones is a listing of the zones and their x/y coordinates along with any 
supporting information to assist in construction and manipulation of matrices

ZoneMapping is a support class for Zones and isn't intended for direct use,
it's for containing information about mapping one zone system to another
(used during disaggregation / aggregation processing)

load_matrix can be used to load either a Matrix or MatrixStack that has been
saved using their save methods.
"""

import operator
import time
import numpy
import pandas
import jsonpickle
import jsonpickle.ext.pandas as jsonpickle_pd
jsonpickle_pd.register_handlers()
import jsonpickle.ext.numpy as jsonpickle_numpy
jsonpickle_numpy.register_handlers()
from pathlib import Path
from pathlib import PurePath
from warnings import warn
from math import sqrt
#TODO fix class decorator and implement?
from matrix_decorators import standard_tools #, class_standard_tools

DATATYPE = numpy.float64
TAB = "    "
MESSAGE_HOOK = print
REFERENCE = 'reference'
X_COORD = 'x_coord'
Y_COORD = 'y_coord'
INDEX = 'index'
O_ZONE_REF = 'o_zone'
D_ZONE_REF = 'd_zone'
VALUES_REF = 'values'
PROP_STR = 'proportions'

class MatrixStackError(Exception):
    pass

class ZonesError(Exception):
    pass

class MatrixError(Exception):
    pass

# @class_standard_tools
class MatrixStack(object):
    """object for managing a stack of matrices
    """
    def __init__(self, 
                 name: str,
                 zones: 'Zones',
                 time_period: str,
                 vehicle_type: str,
                 value_type: str):
        self._name = name
        self._vehicle_type = vehicle_type
        self._zones = zones
        self._time_period = time_period
        self._value_type = value_type
        self._levels = {}
    
    @property
    def name(self) -> str:
        return f'{self._name}'
    
    @name.setter
    def name(self, new_name) -> None:
        self._name = new_name
        
    @property
    def vehicle_type(self) -> str:
        return f'{self._vehicle_type}'
    
    @vehicle_type.setter
    def vehicle_type(self, vehicle_type) -> None:
        '''updates the vehicle type of the stack and the matrices
        only updates the matrix values if they match the stack values
        '''
        MESSAGE_HOOK(f"{self.name} vehicle type updated from "
                     f"{self.vehicle_type} to {vehicle_type}")
        for matrix in self.levels.values():
            if matrix.vehicle_type == self._vehicle_type:
                matrix.vehicle_type = vehicle_type
        self._vehicle_type = vehicle_type
    
    @property
    def zones(self) -> list:
        '''get ordered list of zones in matrix
        '''
        return self._zones.references
    
    @property
    def zone_count(self) -> int:
        '''get count of zones in matrix
        '''
        return len(self._zones)
    
    @property
    def zones_object(self) -> 'Zones':
        '''get the zones object used for the zones for this matrix
        '''
        return self._zones.copy()
    
    @property
    def distances(self) -> 'Matrix':
        '''get a distance matrix generated from the zones
        '''
        return self._zones.create_distance_matrix()
    
    @property
    def time_period(self) -> str:
        return f'{self._time_period}'
    
    @property
    def value_type(self) -> str:
        '''the type of value that is represented in the matrices
        e.g. trips or distance
        '''
        return f'{self._value_type}'
    
    @value_type.setter
    def value_type(self, new_type) -> None:
        '''updates the value type of both the matrix stack and the matrices
        '''
        MESSAGE_HOOK(f"{self.name} value type updated from {self.value_type} "
                     f"to {new_type}")
        self._value_type = new_type
        for matrix in self._levels.values():
            matrix.value_type = new_type
    
    @property
    def level_info(self) -> list:
        '''gets a list containing the info for each matrix in the stack
        '''
        return [(matrix.purpose, matrix.vehicle_type) for 
                matrix in self.matrices]
    
    @property
    def levels(self):
        ''' get a copy of the levels dictionary
        '''
        return dict(self._levels)
    
    @property
    def next_level(self):
        if len(self._levels.keys()) > 0:
            return max(self._levels.keys()) + 1
        else:
            return 1
    
    @property
    def purposes(self):
        '''return a list of all purposes in the stack
        '''
        return self._matrix_attributes("purpose")
    
    @property
    def matrices(self):
        '''return a list of all matrices in the object
        '''
        return list(self.levels.values())
    
    @property
    def values(self) -> 'numpy.ndarray':
        '''returns a square matrix of values stacked in the order of the levels
        '''
        levels = self.levels
        matrices = [levels[key] for key in sorted(levels)]
        output_matrix = None
        for matrix in matrices:
            matrix_values = matrix.values
            if output_matrix is None:
                output_matrix = matrix_values
            else:
                output_matrix = pandas.concat((output_matrix, matrix_values))
        return output_matrix
    
    @property
    def matrix_sums(self):
        '''return a list containing a sum of each matrix in order
        '''
        return self._matrix_attributes("matrix_sum")
    
    @property
    def intrazonal_sums(self):
        '''return a list containing the intrazonal sum of each matrix in order
        '''
        return self._matrix_attributes("intrazonal_sum")
    
    def _matrix_attributes(self, method_name: str):
        '''return a list containing the result from running the given method
        on each matrix in the stack'''
        return [getattr(matrix, method_name) for matrix in self.matrices]
    
    @property
    def file_name(self):
        return (f"{self._name}_{self._time_period}_{self.vehicle_type}_"
                f"{self.value_type}")
    
    def __str__(self):
        return_string = (f"MatrixStack Object {self.name} \n"
                        f"for time period {self.time_period} \n"
                        f"representing value type {self.value_type} \n"
                        f"with {len(self.levels.keys())} levels\n")
        for level, matrix in self.levels.items():
            return_string += f"Level {level}: \n[\n{matrix}]\n"
        return return_string
        
    def __mul__(self, other) -> 'MatrixStack':
        new_matrices = {}
        if isinstance(other, (int, float, numpy.ndarray, Matrix)):
            for level, matrix in self._levels.items():
                new_matrices[level] = matrix * other
        elif isinstance(other, dict):
            for level, matrix in self._levels.items():
                new_matrices[level] = matrix * other[matrix.purpose]
        elif isinstance(other, MatrixStack):
            for level, matrix in self._levels.items():
                if matrix.purpose not in other.purposes:
                    raise MatrixStackError(
                        f"__mul__ receieved a matrix purpose that doesn't "
                        f"exist in secondary matrixstack ({matrix.purpose} "
                        f"not in {other.purposes})")
                new_matrices[level] = (
                    matrix * 
                    other.get_by_purpose(matrix.purpose))
        else:
            return NotImplemented
        return self._copy(matrices = new_matrices)
    
    def __rmul__(self, other) -> 'MatrixStack':
        return self.__mul__(other)
    
    def __truediv__(self, other) -> 'MatrixStack':
        new_matrices = {}
        if isinstance(other, (int, float, numpy.ndarray, Matrix)):
            for level, matrix in self._levels.items():
                new_matrices[level] = matrix / other
        elif isinstance(other, dict):
            for level, matrix in self._levels.items():
                new_matrices[level] = matrix / other[matrix.purpose]
        elif isinstance(other, MatrixStack):
            for level, matrix in self._levels.items():
                if matrix.purpose not in other.purposes:
                    raise MatrixStackError(
                        f"__div__ receieved a matrix purpose that doesn't "
                        f"exist in secondary matrixstack ({matrix.purpose} "
                        f"not in {other.purposes})")
                new_matrices[level] = (
                    matrix / 
                    other.get_by_purpose(matrix.purpose))
        else:
            return NotImplemented
        return self._copy(matrices = new_matrices)
    
    def __rtruediv__(self, other) -> 'MatrixStack':
        new_matrices = {}
        if isinstance(other, (int, float, numpy.ndarray, Matrix)):
            for level, matrix in self._levels.items():
                new_matrices[level] = other / matrix
        elif isinstance(other, dict):
            for level, matrix in self._levels.items():
                new_matrices[level] = other[matrix.purpose] / matrix
        elif isinstance(other, MatrixStack):
            for level, matrix in self._levels.items():
                if matrix.purpose not in other.purposes:
                    raise MatrixStackError(
                        f"__div__ receieved a matrix purpose that doesn't "
                        f"exist in secondary matrixstack ({matrix.purpose} "
                        f"not in {other.purposes})")
                new_matrices[level] = (
                    other.get_by_purpose(matrix.purpose) /
                    matrix)
        else:
            return NotImplemented
        return self._copy(matrices = new_matrices)
    
    def __add__(self, other) -> 'MatrixStack':
        new_matrices = {}
        if isinstance(other, (int, float, numpy.ndarray)):
            for level, matrix in self._levels.items():
                new_matrices[level] = matrix + other
        elif isinstance(other, dict):
            for level, matrix in self._levels.items():
                new_matrices[level] = matrix + other[matrix.purpose]
        elif isinstance(other, MatrixStack):
            for level, matrix in self._levels.items():
                if matrix.purpose not in other.purposes:
                    MESSAGE_HOOK(
                        f"__add__ receieved a matrix purpose that doesn't "
                        f"exist in secondary matrixstack ({matrix.purpose} "
                        f"not in {other.purposes}), leaving as is")
                    new_matrices[level] = matrix
                else:
                    new_matrices[level] = (
                        matrix +
                        other.get_by_purpose(matrix.purpose))
        else:
            return NotImplemented
        return self._copy(matrices = new_matrices)
    
    def __radd__(self, other) -> 'MatrixStack':
        return self.__add__(other)
    
    def __sub__(self, other) -> 'MatrixStack':
        new_matrices = {}
        if isinstance(other, (int, float, numpy.ndarray)):
            for level, matrix in self._levels.items():
                new_matrices[level] = matrix - other
        elif isinstance(other, dict):
            for level, matrix in self._levels.items():
                new_matrices[level] = matrix - other[matrix.purpose]
        elif isinstance(other, MatrixStack):
            for level, matrix in self._levels.items():
                if matrix.purpose not in other.purposes:
                    MESSAGE_HOOK(
                        f"__sub__ receieved a matrix purpose that doesn't "
                        f"exist in secondary matrixstack ({matrix.purpose} "
                        f"not in {other.purposes}), leaving as is")
                    new_matrices[level] = matrix
                else:
                    new_matrices[level] = (
                        matrix -
                        other.get_by_purpose(matrix.purpose))
        else:
            return NotImplemented
        return self._copy(matrices = new_matrices)
    
    def __rsub__(self, other) -> 'MatrixStack':
        new_matrices = {}
        if isinstance(other, (int, float, numpy.ndarray)):
            for level, matrix in self._levels.items():
                new_matrices[level] = other - matrix
        elif isinstance(other, dict):
            for level, matrix in self._levels.items():
                new_matrices[level] =  other[matrix.purpose] - matrix
        elif isinstance(other, MatrixStack):
            for level, matrix in self._levels.items():
                if matrix.purpose not in other.purposes:
                    MESSAGE_HOOK(
                        f"__rsub__ receieved a matrix purpose that doesn't "
                        f"exist in secondary matrixstack ({matrix.purpose} "
                        f"not in {other.purposes}), leaving as is")
                    new_matrices[level] = matrix
                else:
                    new_matrices[level] = (
                        other.get_by_purpose(matrix.purpose) -
                        matrix)
        else:
            return NotImplemented
        return self._copy(matrices = new_matrices)
    
    def copy(self):
        return self._copy()
    
    def _copy(self,
              name: str = None,
              zones: 'Zones' = None,
              time_period: str = None,
              value_type: str = None,
              vehicle_type: str = None,
              matrices: dict = None
              ) -> "MatrixStack":
        copy = MatrixStack(
            name = self.name if name is None else name,
            zones = self.zones_object if zones is None else zones,
            time_period = self.time_period if time_period is None 
                            else time_period,
            vehicle_type = self.vehicle_type if vehicle_type is None 
                            else vehicle_type,
            value_type = self.value_type if value_type is None 
                            else value_type)
        if matrices is None:
            matrices = self.levels
        for level, matrix in matrices.items():
            matrix = matrix._copy(
                zones = self.zones_object if zones is None else zones,
                time_period = self.time_period if time_period is None 
                            else time_period,
                vehicle_type = self.vehicle_type if vehicle_type is None 
                                else vehicle_type,
                value_type = self.value_type if value_type is None 
                                else value_type)
            copy.add_matrix(level, matrix)
        return copy
    
    def _replace_zones_object(
            self,
            new_zones_object: "Zones"
            ) -> "MatrixStack":
        new_matrices = {}
        for level, matrix in self.levels.items():
            new_matrices[level] = matrix._copy(zones = new_zones_object)
        return self._copy(
            zones = new_zones_object,
            matrices = new_matrices)
    
    def invert(self) -> 'MatrixStack':
        if self._value_type != PROP_STR:
            raise MatrixStackError(f'invert tried on MatrixStack that is not '
                                   f'{PROP_STR} value type ({PROP_STR} '
                                   f'value type)')
        ones_array = numpy.ones((len(self.zones), len(self.zones)))
        new_matrices = {}
        for level, matrix in self.levels.items():
            new_matrices[level] = matrix.operate(operator.sub,
                                                 factor = ones_array,
                                                 reverse = True)
        return self._copy(matrices = new_matrices)
    
    def add_matrix(self, 
                   matrix_level: int,
                   matrix: 'Matrix',
                   overwrite: bool = False) -> None:
        if not isinstance(matrix_level, int):
            raise MatrixStackError(
                f"Invalid level type given to add_matrix: {matrix_level} "
                f"of type {type(matrix_level)} is not an int")
        if matrix.zones_object != self.zones_object:
            raise MatrixStackError(
                f"Zones must be the same for all matrices in the "
                f"matrix stack\nGiven {matrix.zones_object} "
                f"({repr(matrix.zones_object)})\nStack zones is "
                f"{self.zones_object} ({repr(self.zones_object)})")
        if matrix.time_period != self.time_period:
            raise MatrixStackError(
                f"The time period of the matrix being added must be the "
                f"same as the stacks time period\nGiven {matrix.time_period}, "
                f"stack is of {self.time_period}")
        if matrix.value_type != self.value_type:
            raise MatrixStackError(
                f"The value type of the matrix being added must be the same "
                f"as the stacks value type\nGiven {matrix.value_type}, "
                f"stack is of {self.value_type}")
        purposes = [matrix.purpose for matrix in self.levels.values()]
        if overwrite == False and matrix.purpose in purposes:
            raise MatrixStackError(
                f"The purpose of the matrix being added to the matrix stack "
                f"already exists; purposes must be unique within a stack\n"
                f"Given {matrix.purpose}, stack contains {purposes}")
        self._levels[matrix_level] = matrix
        matrix.level = matrix_level
        
    def add_disaggregation_map(self,
                                disagg_name: str,
                                disagg_df: 'pandas.DataFrame',
                                columns: dict = None
                                ) -> 'MatrixStack':
        '''adds disaggregation info to every zones object in the stack
        '''
        new_stack = self.copy()
        if columns is None:
            new_stack._zones.add_disaggregation_map(
                disagg_name,
                disagg_df)
        else:
            new_stack._zones.add_disaggregation_map(
                disagg_name,
                disagg_df,
                **columns)
        for matrix in new_stack._levels.values():
            matrix._zones = new_stack.zones_object
        return new_stack
        
    def get_by_purpose(self,
                       purpose: str) -> 'Matrix':
        '''gets a copy of the matrix that matches the given purpose
        '''
        purposes = []
        for level, matrix in self.levels.items():
            purposes.append(matrix.purpose)
            if matrix.purpose == purpose:
                return matrix.copy()
        raise MatrixStackError(
            f"get_by_purpose given purpose not in any matrix ({purpose} "
            f"not in [{purposes}])")
            
    def flatten(self,
                new_level: int = 1,
                new_purpose: str = "",
                new_vehicle_type: str = ""
                ) -> 'Matrix':
        '''Takes all of the matrices in the stack and adds them into a single
        "flattened" matrix, and returns as a matrix object
        '''
        new_matrix = Matrix(self.zones_object,
                            level = new_level,
                            purpose = new_purpose,
                            vehicle_type = new_vehicle_type,
                            time_period = self.time_period)
        for level, matrix in self.levels.items():
            new_matrix += matrix
        return new_matrix
    
    def proportions(self,
                    other: 'MatrixStack',
                    mapping: list,
                    max_value: float = None,
                    output_folder: 'PurePath' = None) -> 'MatrixStack':
        '''generates proportion matrices as a new matrix stack in order of
        the mapping given
        where mapping is [(this_purpose, other purpose)]
        '''
        new_stack = MatrixStack(
            name = self.name,
            zones = self.zones_object,
            vehicle_type = self.vehicle_type,
            time_period = self.time_period,
            value_type = PROP_STR)
        level_count = 0
        for this_purpose, other_purpose in mapping:
            level_count += 1
            matrix_1 = self.get_by_purpose(this_purpose)
            matrix_2 = other.get_by_purpose(other_purpose)
            proportions = matrix_1.generate_proportion_matrix(
                matrix_2,
                max_value = max_value,
                output_folder = output_folder)
            new_stack.add_matrix(level_count, proportions)
        return new_stack
    
    def save(self, 
             output_directory: PurePath,
             prefix: str = "",
             summaries: bool = True) -> str:
        '''saves the matrix_stack object to a json file
        jsonpickle automatically converts the object to a json representation
        use load_matrix(file_path) to restore the object from a file
        '''
        if prefix != "":
            output_file_name = (f"{prefix}_{self.file_name}.json")
        else:
            output_file_name = (f"{self.file_name}.json")
        output_file_location = output_directory / output_file_name
        with open(output_file_location, 'w') as save_file:
            save_file.write(jsonpickle.encode(self,
                                              keys = True))
        if summaries:
            if prefix != "":
                summary_prefix = (f"{prefix}_{self.name}")
            else:
                summary_prefix = (f"{self.name}")
            for level, matrix in self.levels.items():
                matrix.output_summary(output_directory, 
                                      summary_prefix)
        return output_file_location
    
    def save_csv(self,
                 output_directory: PurePath,
                 prefix: str = "",
                 precision: int = 2
                 ) -> str:
        '''saves a saturn-friendly representation of the matrices
        i.e. a csv with a zone-name as the first value and the matrix as a 
        square stacked representation
        '''
        if prefix != "":
            output_file_name = (f"{prefix}_{self.file_name}.csv")
        else:
            output_file_name = (f"{self.file_name}.csv")
        output_file_location = output_directory / output_file_name
        output_matrix = self.values
        output_matrix.to_csv(output_file_location,
                             float_format = f"%.{precision}f",
                             header = False)
        return output_file_location
    
    def disaggregate(self,
                     orig_dest_maps: dict,
                     new_zone_system: 'Zones',
                     output_checking_folder: Path = None
                     ) -> 'MatrixStack':
        '''runs a disaggregation process on every matrix in the stack
        orig_dest_maps needs to be {purpose: (origin_map_name, dest_map_name)}
        '''
        new_matrices = {}
        for purpose, disagg_maps in orig_dest_maps.items():
            matrix = self.get_by_purpose(purpose)
            new_matrices[matrix.level] = matrix.disaggregate_matrix(
                disagg_maps[0], 
                disagg_maps[1], 
                new_zone_system,
                output_checking_folder)
        return self._copy(
            zones = new_zone_system,
            matrices = new_matrices)
    
    def redistribute(self,
                     redist_map_name: str,
                     purpose_trip_ends: dict,
                     distance_filter: float = None
                     ) -> ('MatrixStack', 'MatrixStack'):
        '''runs a redistribution process on every matrix in the stack
        purpose trip end is a dictionary of purposes to trip ends
        each trip end must be 'origin' or 'destination'
        distance filter is used to limit whether the redistribution is only
        used on trips longer than a given length
        '''
        redist_matrices = {}
        remain_matrices = {}
        for purpose, trip_end in purpose_trip_ends.items():
            matrix = self.get_by_purpose(purpose)
            redist_matrix, remain_matrix = matrix.redistribute_matrix(
                redist_map_name,
                trip_end,
                distance_filter)
            redist_matrices[matrix.level] = redist_matrix
            remain_matrices[matrix.level] = remain_matrix
        redist_stack = self._copy(
            matrices = redist_matrices)
        remain_stack = self._copy(
            matrices = remain_matrices)
        return redist_stack, remain_stack
    
    def operate_by_property(self,
                            bands_values: dict,
                            operator_func,
                            bandvalue_matrix: 'Matrix',
                            reverse: bool = False
                            ) -> 'MatrixStack':
        '''uses operate by property on each matrix
        see Matrix help for operate_by_property
        '''
        new_matrices = {}
        for level, matrix in self.levels.items():
            new_matrices[level] = matrix.operate_by_property(
                bands_values,
                operator_func,
                bandvalue_matrix,
                reverse)
        return self._copy(
            matrices = new_matrices)
    
    def get_distributions(
            self,
            brackets: list,
            values_matrix: 'Matrix' = None,
            proportions: bool = False,
            intrazonals_bracket: bool = True,
            return_format: str = 'dict',
            field_name: str = None
            ) -> dict:
        '''gets {purpose: distribution} for the stack
        or a dataframe with the same information, depending on return_format
        
        values_matrix will be a distance matrix if not given
        proportions means the outcome will be in proportions instead of values
        intrazonals bracket will create a (0, 1) bracket for values to capture
        intrazonal values (as it's assumed these are a value of 0)
        distributions are provided back for each bracket given
        e.g. if a trip length distribution is sought, then the values_matrix
        should be a distance matrix, and the brackets will be the break points
        in distribution. 
        if field_name is given, runs zone_field_tlds instead which will
            generate a raft of distributions for every unique value in the
            given field. can be used to generate tlds for every sector or 
            similar.
        
        example:
            for a stack with a single matrix of 'purpose1' purpose,
                with a matrixarray of values.
                when calculating values in range, it is looking at the 
                matrix given, in this case the distance matrix
            inputs:
                brackets = [2000, 4000, 6000]
                values_matrix = None (will create distance matrix from Zones)
                proportions = False
                intrazonals_bracket = True
            returns:
                if return_format == 'dict':
                    {purpose1: {(0, 1): values in 0-1 range,
                                (1, 2000): values in 1-2000 range,
                                (2000, 4000): values in 2000-4000 range,
                                (4000, 6000): values in 4000-6000 range,
                                (6000, 1000000000): value}}
                elif return_format == 'DataFrame':
                    pandas dataframe with brackets as index,
                    and values per purpose as columns
                    plus a final column with a sum of all values
        '''
        if return_format == 'dict':
            distributions = {}
            for level, matrix in self.levels.items():
                distributions[matrix.purpose] = matrix.get_tld(
                    brackets = brackets,
                    distances = values_matrix,
                    proportions = proportions,
                    intrazonals_bracket = intrazonals_bracket)
            return distributions
        elif return_format == 'DataFrame':
            tld_df = None
            for level, matrix in self.levels.items():
                if field_name is None:
                    tld = matrix.get_tld(
                        brackets = brackets,
                        distances = values_matrix,
                        proportions = proportions,
                        intrazonals_bracket = intrazonals_bracket,
                        return_format = return_format)
                else:
                    tld = matrix.zone_field_tlds(
                        field_name = field_name,
                        brackets = brackets,
                        distances = values_matrix,
                        proportions = proportions,
                        intrazonals_bracket = intrazonals_bracket,
                        return_format = return_format)
                if tld_df is None:
                    tld_df = tld
                else:
                    tld_df = tld_df.join(tld)
            if proportions:
                if field_name is None:
                    flat_prop = self.flatten().get_tld(
                        brackets = brackets,
                        distances = values_matrix,
                        proportions = proportions,
                        intrazonals_bracket = intrazonals_bracket,
                        return_format = return_format)
                    tld_df['total_props'] = flat_prop
                else:
                    flattened = self.flatten()
                    flattened.purpose = 'flat'
                    flat_prop = flattened.zone_field_tlds(
                            field_name = field_name,
                            brackets = brackets,
                            distances = values_matrix,
                            proportions = proportions,
                            intrazonals_bracket = intrazonals_bracket,
                            return_format = return_format)
                    tld_df = tld_df.join(flat_prop)
            else:
                if field_name is None:
                    tld_df['sum_total'] = tld_df.loc[:, self.purposes].sum(
                        axis = 1)
            return tld_df
        else:
            raise MatrixStackError(
                f'get_distributions given unknown return_format: '
                f'{return_format}')
    
    def aggregate_by_zone_field(self, 
                                zone_field_name: str,
                                new_zone_system: 'Zones' = None,
                                calc_type: str = 'sum',
                                retention: dict = None
                                ) -> 'MatrixStack':
        '''Runs aggregate_by_zone_field on each individual matrix within the
        stack
        '''
        new_matrices = {}
        for level, matrix in self.levels.items():
            new_matrices[level] = matrix.aggregate_by_zone_field(
                zone_field_name,
                new_zone_system,
                calc_type,
                retention)
        return self._copy(
            name = f"{self.name}_{zone_field_name}",
            zones = self.zones_object.summarise_by_field(
                zone_field_name,
                retention),
            matrices = new_matrices)
    
    def zero_intrazonals(self) -> 'MatrixStack':
        new_matrices = {}
        for level, matrix in self.levels.items():
            new_matrices[level] = matrix.zero_intrazonals()
        return self._copy(
            matrices = new_matrices)
    
    def save_user_classes_csv(self,
                              output_folder: 'PurePath',
                              output_prefix: str,
                              ) -> 'MatrixStack':
        """
        save each user class matrix to separate csv files
        """
        for purpose in self.purposes:
            matrix = self.get_by_purpose(purpose)
            matrix.save_csv(output_folder, output_prefix)



# @class_standard_tools
class Zones(object):
    """object for storing information about spatial zones
    """
    _core_attributes = [REFERENCE, X_COORD, Y_COORD]
    def __init__(self, 
                 zone_system_name: str,
                 zones_file: PurePath,
                 zones_sheet: str = "",
                 column_overrides: dict = None,
                 **metadata: str) -> None:
        """Imports a set of zones from a zone file and populates the zone list
        
        zone_system_name is a text descriptor
        zones_file is the location of the zones file (xlsx or csv)
        zones_sheet is used if the file is an xlsx file
        column_overrides is {column_header: actual_column_header}
            where column_header is the name used by the Zones class
            (only operates on the core attributes)
            e.g. {REFERENCE: "zones_ref"} would tell the Zones class
            to use "zones_ref" in place of the defined REFERENCE name
        """
        if column_overrides is not None:
            if (len(column_overrides) > len(self._core_attributes) or
                len([attribute for attribute in column_overrides.keys() 
                     if attribute not in self._core_attributes]) > 0):
                raise ZonesError(
                    f"zone_column_overrides needs to only contain keys "
                    f"from {self._core_attributes} (given {column_overrides})")
        if zones_file is not None:
            zone_df = dataframe_import(zones_file, zones_sheet)
            missing_attributes = []
            for attribute in list(self._core_attributes):
                if (column_overrides is not None and
                    attribute in column_overrides.keys()):
                    attribute = column_overrides[attribute]
                if (attribute not in zone_df.columns and
                    attribute not in zone_df.index.names):
                    missing_attributes.append(attribute)
            if len(missing_attributes) > 0:
                raise ZonesError(
                    f"zones file given does not define a {missing_attributes} "
                    f"column(s), please amend")
                
            if column_overrides is not None:
                overrides = {value: key for key, value in 
                             column_overrides.items()}
                zone_df = zone_df.rename(columns = overrides, 
                                         errors='raise')
            duplicates = zone_df.duplicated(REFERENCE)
            if duplicates.any():
                raise ZonesError(
                    f"zones file given has duplicate zone references:\n"
                    f"{zones_file}\n{zone_df.loc[duplicates]}")
            
            zone_df.index.rename(INDEX, inplace=True)
            zone_df = zone_df.reset_index()
            zone_df.set_index(REFERENCE, inplace=True)
            
            self._zones = zone_df
            self._source_file = zones_file
            self._source_sheet = zones_sheet
        self._name = zone_system_name
        self._disaggregation_maps = {}
        for name, value in metadata.items():
            setattr(self, f"_{name}", value)
    
    def __str__(self) -> str:
        return_string = "Zones Object\n"
        for key, value in self.__dict__.items():
            if key != "_zones" and key != "_disaggregation_maps":
                return_string += f"{key[1:]}: {value}\n"
        return_string = f"{return_string}zone_count: {len(self._zones)}\n"
        return_string = f"{return_string}indices: {self._zones.index.names}\n"
        return_string = (f"{return_string}fields: "
                         f"{self._zones.columns.tolist()}\n")
        disagg_summary = [key for key in self.disaggregation_maps.keys()]
        return_string = (f"{return_string}disagg_maps (type, count of maps): "
                         f"{disagg_summary}")
        return return_string
    
    def __len__(self) -> int:
        return len(self._zones)
    
    def __eq__(self, other: 'Zones') -> bool:
        ''' tests equality of the two objects
        '''
        if isinstance(other, self.__class__):
            for key, value in self.__dict__.items():
                if key not in other.__dict__.keys():
                    return False
                if not isinstance(other.__dict__[key], type(value)):
                    return False
                if isinstance(value, pandas.DataFrame):
                    if not value.equals(other.__dict__[key]):
                        return False
                else:
                    if value != other.__dict__[key]:
                        return False
            return True
        else:
            return False
    
    @property
    def name(self) -> str:
        return f'{self._name}'
    
    @property
    def source(self) -> tuple:
        return (Path(self._source_file), f'{self._source_sheet}')
    
    @property
    def references(self) -> list:
        return self._zones.index.tolist()
    
    @property
    def columns(self) -> list:
        return list(self._zones.columns)
    
    @property
    def disaggregation_maps(self) -> dict:
        return dict(self._disaggregation_maps)
    
    @property
    def disaggregation_maps_strings(self) -> dict:
        return {mapping_type: {zone: str(zonemap) for 
                zone, zonemap in zoneinfo.items()}
                for mapping_type, zoneinfo in
                self._disaggregation_maps.items()}
    
    @property
    def metadata(self) -> dict:
        metadata = {}
        inbuilts = ['_zones', '_name', '_source_file', 
                    '_source_sheet', '_disaggregation_maps']
        for name, value in self.__dict__.items():
            if name not in inbuilts:
                # strip leading underscore to make sure 
                # new metadata value doesn't have multiple underscores
                metadata[f'{name[1:]}'] = value
        return metadata
    
    @property
    def zone_df(self) -> 'pandas.DataFrame':
        return self._zones.copy()
    
    def copy(self) -> 'Zones':
        return self._copy()
    
    def _copy(self, 
              zone_system_name: str = None,
              zones_file: PurePath = None,
              zones_sheet: str = "",
              import_disaggregation: bool = True,
              zone_df: 'pandas.DataFrame' = None,
              **metadata: str):
        metadata = self.metadata if len(metadata) == 0 else metadata
        if (((zones_file == self.source[0]) or 
             (zones_file is None)) and 
            ((zones_sheet == self.source[1]) or 
             (zones_sheet == ""))):
            zone_copy = Zones(
                zone_system_name = self.name if zone_system_name is None 
                                    else zone_system_name,
                zones_file = None,
                zones_sheet = None,
                **metadata)
            if zone_df is not None:
                zone_copy._zones = zone_df
                zone_copy._source_file = self._source_file
                zone_copy._source_sheet = self._source_sheet
            else:
                zone_copy._import_zones(self._zones,
                                        self._source_file,
                                        self._source_sheet)
            if (import_disaggregation == True):
                zone_copy._import_disaggregation_data(self.disaggregation_maps)
        else:
            zone_copy = Zones(
                zone_system_name = self.name if zone_system_name is None 
                                    else zone_system_name,
                zones_file = zones_file,
                zones_sheet = zones_sheet,
                **metadata)
        return zone_copy
    
    def _import_zones(self, zone_df,
                      source_file,
                      source_sheet):
        self._zones = zone_df
        self._source_file = source_file
        self._source_sheet = source_sheet
        
    def _import_disaggregation_data(self, disagg_dict):
        self._disaggregation_maps = disagg_dict
    
    def get_zone_details(self, 
                         zone_reference: str) -> pandas.Series:
        return self._zones.loc[zone_reference]
    
    def get_column_values(self,
                          column_name: str
                          ) -> pandas.Series:
        """returns a series containing the values for the given column name
        """
        if column_name not in self.columns:
            raise ZonesError(f"get_column_values given column name not in "
                             f"columns ({column_name} not in {self.columns})")
        return self._zones.loc[:, column_name]

    def get_index(self, zone_reference) -> int:
        return self._zones.loc[zone_reference, INDEX]
    
    def get_coords(self, zone_reference) -> (float, float):
        """Gets the coordinates for a given zone
        """
        return (self._zones.loc[zone_reference, X_COORD], 
                self._zones.loc[zone_reference, Y_COORD])
    
    def get_distance(self, zone_ref_1, zone_ref_2) -> float:
        """Gets the straight-line distance between two zones
        """
        return self._measure_distance(self.get_coords(zone_ref_1), 
                                      self.get_coords(zone_ref_2))
    
    def _measure_distance(self,
                         pos1: (float, float),
                         pos2: (float, float)) -> float:
        x_dist = pos2[0] - pos1[0]
        y_dist = pos2[1] - pos1[1]
        return sqrt(x_dist ** 2 + y_dist ** 2)
    
    def create_distance_matrix(self) -> 'Matrix':
        '''constructs a new matrix based on the crow-flies distances between
        the x and y positions given for each zone
        '''
        x_coords = self.zone_df.loc[:,X_COORD].tolist()
        y_coords = self.zone_df.loc[:,Y_COORD].tolist()
        x_origin, x_dest = numpy.meshgrid(x_coords, x_coords, 
                                          indexing = 'ij')
        y_origin, y_dest = numpy.meshgrid(y_coords, y_coords, 
                                          indexing = 'ij')
        x_sq = (x_origin.astype('int64') - x_dest.astype('int64')) ** 2
        y_sq = (y_origin.astype('int64') - y_dest.astype('int64')) ** 2
        added = x_sq + y_sq
        distance_matrix = numpy.sqrt(added)
        return Matrix(
            self, 
            matrix_array = distance_matrix, 
            comments = 'Created by create_distance_matrix from zones',
            value_type = 'Distance (m)')
    
    def create_matrix_from_column(
            self,
            column_header: 'str',
            value_type: 'str' = '',
            result_type: str = 'row',
            value_lookup: dict = None,
            value_lookup_x: dict = None,
            value_lookup_y: dict = None,
            invert: bool = False
            ) -> 'Matrix':
        '''Creates a new matrix based on a column
        column_header is the name of the column used in the zone dataframe
        value_type is the value type of the matrix returned
        
        result_type is one of row, column, mul, add
            row provides a row-value-based matrix
            column provides a column-value-based matrix
            mul will multiply the resulting x and y matrices
            add will add the resulting x and y matrices
            min will apply numpy.minimum to the x and y matrices
            max will apply numpy.maximum to the x and y matrices
        
        value_lookup allows the column values to be replaced by something else
            e.g. mapping the column values to another value
        
        value_lookup_x and _y allow different lookup lists to be used for
            each of the x and y directions, similar to value_lookup but 
            independent of each other instead of using the same list for both
        '''
        if column_header not in self._zones.columns:
            raise ZonesError(f'create_matrix_from_column: column header not '
                             f'in columns for zones object ({column_header})')
        values = self._zones.loc[:, column_header]
        values_x = values
        values_y = values
        if value_lookup is not None:
            check_lists_equal(values.unique().tolist(),
                              value_lookup.keys(),
                              'column_uniques',
                              'lookup_replacements',
                              'Zones create_matrix_from_column')
            values = values.map(value_lookup).tolist()
            values_x = values
            values_y = values
        if value_lookup_x is not None and value_lookup_y is not None:
            check_lists_equal(values.unique().tolist(),
                              value_lookup_x.keys(),
                              'column_uniques',
                              'lookup_replacements (x)',
                              'Zones create_matrix_from_column')
            values_x = values.map(value_lookup_x).tolist()
            check_lists_equal(values.unique().tolist(),
                              value_lookup_y.keys(),
                              'column_uniques',
                              'lookup_replacements (y)',
                              'Zones create_matrix_from_column')
            values_y = values.map(value_lookup_y).tolist()
        data1, data2 = numpy.meshgrid(values_x, values_y, 
                                      indexing = 'ij')
        if result_type == 'row':
            final_data = data1
        elif result_type == 'column':
            final_data = data2
        elif result_type == 'mul':
            final_data = data1 * data2
        elif result_type == 'add':
            final_data = data1 + data2
        elif result_type == 'min':
            final_data = numpy.minimum(data1, data2)
        elif result_type == 'max':
            final_data = numpy.maximum(data1, data2)
        else:
            raise ZonesError(f"result_type must be in "
                        f"['row', 'column', 'add', 'mul', 'min', 'mul']. "
                        f"current value = {result_type}")
        if invert:
            ones_matrix = numpy.ones((len(self), len(self)))
            final_data = ones_matrix - final_data
        return Matrix(self, 
                      matrix_array = final_data, 
                      comments = ('Created using create_matrix_from_column '
                                  'from Zones'),
                      value_type = value_type)
    
    def create_zero_matrix(self) -> 'numpy.ndarray':
        '''creates a square matrix of zero values the size of the zones list
        '''
        total_zones = len(self.references)
        matrix = numpy.zeros((total_zones, total_zones))
        return matrix
    
    def create_odlist_dataframe(self,
                o_ref: str = O_ZONE_REF,
                d_ref: str = D_ZONE_REF,
                column: str = 'zone') -> 'pandas.Dataframe':
        '''creates a dataframe of the origin and destination zones where 
        all combinations of o to d are provided as a template
        '''
        if column == 'zone':
            values = self.references
        elif column in self.columns:
            values = self._zones[column].tolist()
        else:
            raise ZonesError(f"Column given to create_odlist_dataframe "
                             f"doesn't exist ({column} not in {self.columns})")
        o_list = []
        d_list = []
        for o_value in list(values):
            for d_value in list(values):
                o_list.append(o_value)
                d_list.append(d_value)
        df_dict = {o_ref: o_list,
                   d_ref: d_list}
        list_df = pandas.DataFrame(df_dict)
        return list_df
    
    def summary(self,
                additional_data: dict = None) -> 'pandas.DataFrame':
        '''dumps a summary of the zone data as a dataframe
        appends any data given to the dataframe
        additional data is a dict of {field name: [data]}
            where the data must be of the same length, in the same order
            as the zones list
        '''
        temp_df = self._zones.copy()
        if not additional_data is None:
            for field_name, data in additional_data.items():
                if len(data) != len(temp_df):
                    raise ZonesError(f'summary: additional data given is not '
                                     f'the same length as the zone system\n'
                                     f'(zone system has {len(temp_df)} '
                                     f'entries vs data having {len(data)})')
                temp_df[field_name] = data
        return temp_df
    
    def import_disaggregation_map(self,
                                  disagg_name: str,
                                  disagg_path: PurePath,
                                  disagg_sheetname: str = "",
                                  from_zone: str = 'from_zone',
                                  to_zone: str = 'to_zone',
                                  value: str = 'proportion'
                                  ) -> None:
        '''imports disaggregation data from a given file
        then uses add_disaggregation_map to add it to the Zones object
        '''
        disagg_df = dataframe_import(disagg_path, disagg_sheetname)
        self.add_disaggregation_map(disagg_name,
                                    disagg_df,
                                    from_zone,
                                    to_zone,
                                    value)
        
    def add_disaggregation_map(self,
                               disagg_name: str,
                               disagg_df: 'pandas.DataFrame',
                               from_zone: str = 'from_zone',
                               to_zone: str = 'to_zone',
                               value: str = 'proportion'
                               ) -> None:
        '''imports a table containing information matching zones to new zones
        with values allowing calculation of proportions for each
        stores the disaggregation map that results as {zone_ref: ZoneMapping}
        '''
        if disagg_name in self.disaggregation_maps:
            raise ZonesError(f"add_disaggregation_map tried to add a map name "
                             f"that already exists: {disagg_name}")
        disagg_start_zones = disagg_df[from_zone].unique()
        check_lists_equal(self.references, disagg_start_zones,
                          'zone_references', 'disaggregation_zones',
                          f'Zones import_disaggregation_map for {disagg_name}')
        
        disagg_map = {}
        for row in disagg_df.iterrows():
            row = row[1]
            if row[from_zone] not in disagg_map.keys():
                disagg_map[int(row[from_zone])] = ZoneMapping(
                    int(row[from_zone]))
            disagg_map[int(row[from_zone])].add_target(int(row[to_zone]),
                                                  row[value])
        disagg_map_proportions = {}
        for zone, zone_mapping in disagg_map.items():
            disagg_map_proportions[zone] = zone_mapping.proportions()
        self._disaggregation_maps[disagg_name] = disagg_map_proportions
        
    def create_info_list(self,
                           info_type: str,
                           info_map,
                           nan_replacement = 1) -> list:
        '''creates a list of values from the info dict
        info_type must be a column in the zones dataframe
        info_dict is {info_value: list_value}
        '''
        info_col = self.get_column_values(info_type)
        if isinstance(info_map, dict):
            check_lists_equal(list(info_map.keys()), info_col.unique(),
                              'info_map_keys', 'info_col_uniques',
                              'create_info_list', nan_ok = True)
            info_mapping = list(info_col.map(info_map))
            info_mapping = [item if not numpy.isnan(item) 
                            else nan_replacement for item in info_mapping]
            return info_mapping
        
    def summarise_by_field(self,
                           field_name: str,
                           retention: dict = None
                           ) -> 'Zones':
        '''returns a new zones object based around the field given
        carries out a mean of x and y coords (to calculate centroids)
        retention is {output_column: pandas.NamedAgg(
                        column=current_column, aggfunc=func)} 
            where the NamedAgg determines how the column is summarised.
            A single current column can have more than one output column
            associated with it. 
            See https://pandas.pydata.org/pandas-docs/stable/user_guide/groupby.html#named-aggregation
        '''
        if field_name not in self.columns:
            raise ZonesError(f'summarise_by_field given field not in columns '
                             f'({field_name} not in {self.columns})')
        if retention is not None:
            if X_COORD not in retention.keys():
                retention[X_COORD] = pandas.NamedAgg(
                    column=X_COORD, aggfunc=numpy.mean)
            if Y_COORD not in retention.keys():
                retention[Y_COORD] = pandas.NamedAgg(
                    column=Y_COORD, aggfunc=numpy.mean)
            missing = []
            for column, aggregation_type in retention.items():
                if column not in self.columns:
                    missing.append(field_name)
            if len(missing) > 0:
                raise ZonesError(f'summarise_by_field given retention dict '
                                 f'which is missing some of the given fields '
                                 f'{missing} not in {self.columns}')
        else:
            retention = {
                X_COORD: pandas.NamedAgg(column=X_COORD, aggfunc=numpy.mean),
                Y_COORD: pandas.NamedAgg(column=Y_COORD, aggfunc=numpy.mean)}
            
        new_df = self.zone_df.reset_index().set_index(field_name)
        new_df = new_df.loc[:, list(retention.keys())]
        new_df = new_df.groupby(level=0).agg(**retention)
        return self._copy(name = f"{self.name}_{field_name}",
                          zone_df = new_df,
                          import_disaggregation = False)
    
    def create_filter_matrix(self,
                             field_name: str,
                             row_values: list,
                             col_values: list
                             ) -> 'numpy.ndarray':
        """Generates a boolean matrix which can be used as a multiplier
        for any other matrix to filter it down to those cells that match the
        given criteria
        """
        if field_name not in self.columns:
            raise ZonesError(f'create_filter_matrix given field not in '
                             f'column ({field_name} not in {self.columns})')
        field_series = self.get_column_values(field_name)
        lookup_list_row = field_series.isin(row_values)
        lookup_list_col = field_series.isin(col_values)
        origin, dest = numpy.meshgrid(lookup_list_row, lookup_list_col)
        return origin * dest
    
    def get_position(self,
                     zone_ref: str
                     ) -> int:
        '''provides the position of the given reference in the zones list
        this should tie up with the row or column number for the matrix
        '''
        references = [str(ref) for ref in self.references]
        return references.index(zone_ref)
        
#intentionally not using standard_tools decorator as it would be spammy
class ZoneMapping(object):
    '''object to store information about how a single zone maps to a series of 
    targets in a new zone system
    
    targets are stored as {zone_ref: target_proportion}
    '''
    def __init__(self, zone_ref) -> None:
        self._zone = zone_ref
        self._targets = {}
        self._target_min_zone_idx = -1
        self._target_max_zone_idx = -1
        
    def __str__(self):
        return (f'ZoneMapping object: {self.zone}\n{self.count} targets with '
                f'sum of {self.target_sum}')
    
    @property
    def zone(self) -> str:
        return self._zone
    
    @property
    def targets(self) -> dict:
        return dict(self._targets)
    
    @property
    def target_zones(self) -> list:
        return list(self._targets.keys())
    
    @property
    def target_values(self) -> list:
        return list(self._targets.values())
    
    @property
    def target_sum(self) -> float:
        return sum(self._targets.values())
    
    @property
    def count(self) -> int:
        return len(self._targets)
    
    @property
    def min_target_index(self):
        return self._target_min_zone_idx
    
    @property
    def max_target_index(self):
        return self._target_max_zone_idx
    
    def copy(self) -> 'ZoneMapping':
        mapping_copy = ZoneMapping(self.zone)
        mapping_copy._targets = self.targets
        mapping_copy._target_min_zone_idx = self._target_min_zone_idx
        mapping_copy._target_max_zone_idx = self._target_max_zone_idx 
        return mapping_copy
    
    def sort(self, 
             sort_list: list = None
             ) -> None:
        if sort_list is None:
            sort_list = self._targets.keys()
        else:
            check_lists_equal(self._targets.keys(), sort_list,
                              'target_keys', 'sort_list',
                              'ZoneMapping.sort')
        self._targets = {key: self._targets[key]
                         for key in sorted(sort_list)}
    
    def add_target(self, target_zone_ref, value: float) -> None:
        if target_zone_ref in self._targets.keys():
            self.targets[target_zone_ref] += value
        else:
            self._targets[target_zone_ref] = value
        
    def remove_target(self, target_zone_ref) -> None:
        del self._targets[target_zone_ref]
        
    def set_min_max_index(self, zones_list) -> None:
        self._target_min_zone_idx = len(zones_list) + 1
        self._target_max_zone_idx = -1
        for zone in self.target_zones:
            index = zones_list.index(zone)
            if index < self._target_min_zone_idx:
                self._target_min_zone_idx = index
            if index > self._target_max_zone_idx:
                self._target_max_zone_idx = index
            
    def infill_blanks(self, 
                      zones_list: list, 
                      infill_value: float = 0) -> None:
        if self._target_min_zone_idx == -1 or self._target_max_zone_idx == -1:
            raise ZonesError(
                "ZoneMapping infill_blanks requires min and max indices to "
                "be set prior to running this")
        new_targets = {}
        current_zones = self.target_zones
        for number in range(self._target_min_zone_idx, 
                            self._target_max_zone_idx + 1):
            if zones_list[number] in current_zones:
                new_targets[zones_list[number]] = (
                    self._targets[zones_list[number]])
            else:
                new_targets[zones_list[number]] = infill_value
        self._targets = new_targets
        
    def proportions(self) -> 'ZoneMapping':
        '''reproportions all of the target zone values so they sum to 1
        '''
        total = self.target_sum
        if self.count == 0:
            raise ZonesError("proportions called on ZoneMapping object that "
                             "doesn't have any target value")
        if self.target_sum == 0:
            warn(f"proportions called on a ZoneMapping ({self.zone}) "
                 f"with a target sum of 0")
        proportions = {}
        for ref, value in self.targets.items():
            if total > 0:
                proportions[ref] = value / total
            else:
                proportions[ref] = 1 / self.count
        new_obj = ZoneMapping(self.zone)
        for target, value in proportions.items():
            new_obj.add_target(target, value)
        return new_obj
    
    def get_target_proportion_list(
            self,
            new_zones: list
            ) -> list:
        """generates a list of the proportions as they apply to a given
        list of zones
        
        useful for operations such as meshgrid
        """
        proportions_list = []
        index = 0
        for zone in new_zones:
            index += 1
            if zone in self._targets.keys():
                proportions_list.append(self._targets[zone])
            else:
                proportions_list.append(0)
        return proportions_list

# @class_standard_tools
class Matrix(object):
    '''object for storing 2d-square matrix-style information
    '''
    def __init__(self, 
                 zones: Zones, 
                 level: int = 1, 
                 purpose: str = "",
                 time_period: str = "",
                 vehicle_type: str = "",
                 value_type: str = "Trips",
                 comments: str = "",
                 matrix_array = None) -> None:
        self._zones = zones.copy()
        self._check_matrix(matrix_array, zones.references)
        self._populate_info(level, purpose, time_period, 
                            vehicle_type, value_type, comments)

    def _check_matrix(self, matrix_array, zones: list) -> None:
        if matrix_array is None:
            self._matrix = numpy.zeros((len(zones), len(zones)), DATATYPE)
        elif not isinstance(matrix_array, numpy.ndarray):
            raise MatrixError(
                f"Matrix given to __init__ must be a numpy array "
                f"(given {type(matrix_array)})")
        elif not numpy.issubdtype(matrix_array.dtype, numpy.number):
            raise MatrixError(
                f"Matrix given an array with a datatype that is "
                f"not a numeric data type (given {matrix_array.dtype})")
        else:
            self._matrix = matrix_array
    
    def _populate_info(self, level, purpose, time_period, 
                      vehicle_type, value_type, comments):
        self._level = level
        self._purpose = purpose
        self._time_period = time_period
        self._vehicle_type = vehicle_type
        self._value_type = value_type
        self._comments = comments
        
    @property
    def level(self):
        '''get the level this matrix represents 
        (assuming a multi-level matrix)
        '''
        return int(self._level)
    
    @level.setter
    def level(self, level):
        if level != self._level:
            MESSAGE_HOOK(
                f'level for {self.short_name} updated from {self._level} to '
                f'{level}')
            self._level = level
    
    @property
    def purpose(self):
        '''get trip purpose this matrix represents
        '''
        return f'{self._purpose}'
    
    @purpose.setter
    def purpose(self, purpose):
        MESSAGE_HOOK(
            f'purpose for {self.short_name} updated from {self._purpose} to '
            f'{purpose}')
        self._purpose = purpose
    
    @property
    def time_period(self):
        return f'{self._time_period}'
    
    @time_period.setter
    def time_period(self, time_period):
        MESSAGE_HOOK(
            f'time period for {self.short_name} updated from '
            f'{self._time_period} to {time_period}')
        self._time_period = time_period
    
    @property
    def vehicle_type(self):
        return f'{self._vehicle_type}'
    
    @vehicle_type.setter
    def vehicle_type(self, vehicle_type):
        MESSAGE_HOOK(
            f'vehicle type for {self.short_name} updated from '
            f'{self._vehicle_type} to {vehicle_type}')
        self._vehicle_type = vehicle_type
    
    @property
    def value_type(self):
        '''get what the values in the matrix represent 
        (e.g. trips, distance, etc)
        '''
        return f'{self._value_type}'
    
    @value_type.setter
    def value_type(self, value_type):
        MESSAGE_HOOK(
            f'value type for {self.short_name} updated from '
            f'{self._value_type} to {value_type}')
        self._value_type = value_type
        
    @property
    def matrix(self):
        '''get the matrix of values
        '''
        return self._matrix.copy()
    
    @property
    def dim(self):
        '''get dimension (row x col) of matrix as tuple
        '''
        return self._matrix.shape
    
    @property
    def size(self):
        ''' get the count of total number of values in the matrix
        '''
        return self._matrix.size
    
    @property
    def rows(self):
        '''get number of rows
        '''
        return self.dim[0]
    
    @property
    def row_totals(self):
        '''get sum for each row as a list
        '''
        return self._matrix.sum(axis=1)
    
    @property
    def cols(self):
        '''get number of cols
        '''
        return self.dim[1]
    
    @property
    def col_totals(self):
        '''get sum for each col as a list
        '''
        return self._matrix.sum(axis=0)
    
    @property
    def zones(self):
        '''get ordered list of zones in matrix
        '''
        return self._zones.references
    
    @property
    def zone_count(self):
        '''get count of zones in matrix
        '''
        return len(self._zones)
    
    @property
    def zones_object(self):
        '''get the zones object used for the zones for this matrix
        '''
        return self._zones.copy()
    
    @property
    def matrix_sum(self):
        '''get total sum of the matrix
        '''
        return DATATYPE(self._matrix.sum())
    
    @property
    def intrazonals(self):
        '''get an array of intrazonal values
        '''
        return self._matrix.diagonal()
    
    @property
    def intrazonal_sum(self):
        '''get intrazonal sum of the matrix
        '''
        return self._matrix.diagonal().sum()
    
    @property
    def interzonal_sum(self):
        '''get interzonal (non-intrazonal) sum of matrix
        '''
        return self.matrix_sum - self.intrazonal_sum
    
    @property
    def file_name(self):
        '''Creates a file reference based on the matrix properties
        '''
        return (f'{self.time_period}_{self.level}_{self.purpose}_'
                f'{self.vehicle_type}_{self.value_type}')
    
    @property
    def short_name(self):
        '''returns a summarised name for use in messages
        '''
        return self.file_name
    
    @property
    def comments(self):
        return f'{self._comments}'
    
    @property
    def matrix_for_csv(self) -> 'pandas.DataFrame':
        df = pandas.DataFrame(
            index = self.zones,
            data = self.matrix)
        return df

    @property
    def values(self) -> 'numpy.ndarray':
        return self.matrix_for_csv
    
    def __str__(self) -> str:
        return self.get_string(False)
    
    def get_string(self, verbose:bool = True) -> str:
        return_string = "Matrix Object\n"
        return_string = f"{return_string}{self.get_properties()}"
        if verbose:
            return_string = f"{return_string}{self.get_comments()}"
        return return_string
    
    def get_properties(self, 
                       return_type: str = "string"):
        """Gets the properties of the matrix
        """
        properties_list = ['vehicle_type', 'value_type', 'time_period', 
                           'purpose', 'level', 'zone_count',
                           'matrix_sum', 'interzonal_sum', 'intrazonal_sum']
        values_list = [getattr(self, property_type) for 
                       property_type in properties_list]
        try:
            if return_type == "string":
                return_data = ""
                for property_type in properties_list:
                    return_data = (
                        f"{return_data}{property_type}: "
                        f"{getattr(self, property_type)}\n")
            elif return_type == "DataFrame":
                return_data = pandas.DataFrame({'property': properties_list,
                                                'value': values_list})
            else:
                raise MatrixError(f"get_properties given unknown return_type "
                                  f"given {return_type}")
        except ValueError:
            print(property_type)
            raise
        return return_data
    
    def get_comments(self) -> str:
        '''returns a formatted version of the comments
        where any square brackets are used to tab/untab blocks of comment
        '''
        if self._comments == "":
            return self._comments
        comments_lines = self._comments.splitlines()
        indent_level = 0
        comments = []
        for line in comments_lines:
            line = line.strip()
            if line == "]":
                indent_level -= 1
            comments.append(f'{TAB * indent_level}{line}\n')
            if line == "[":
                indent_level += 1
        return_string = "Comments:\n"
        for line in comments:
            return_string += line
        return return_string
    
    def __mul__(self, other) -> 'Matrix':
        '''multiplies the matrix
        '''
        if isinstance(other, (int, float, numpy.ndarray)):
            return self.operate(operator_func = operator.mul,
                                factor = other)
        elif isinstance(other, Matrix):
            return self.operate(operator_func = operator.mul,
                                matrix = other)
        else:
            return NotImplemented
            
    def __rmul__(self, other) -> 'Matrix':
        return self.__mul__(other)
    
    def __truediv__(self, other) -> 'Matrix':
        '''divides the matrix
        '''
        if isinstance(other, (int, float, numpy.ndarray)):
            return self.operate(operator_func = operator.truediv,
                                factor = other)
        elif isinstance(other, Matrix):
            return self.operate(operator_func = operator.truediv,
                                matrix = other)
        else:
            return NotImplemented
    
    def __rtruediv__(self, other) -> 'Matrix':
        '''divides by the matrix
        '''
        if isinstance(other, (int, float, numpy.ndarray)):
            return self.operate(operator_func = operator.truediv,
                                factor = other,
                                reverse = True)
        elif isinstance(other, Matrix):
            return self.operate(operator_func = operator.truediv,
                                matrix = other,
                                reverse = True)
        else:
            return NotImplemented
    
    def __add__(self, other) -> 'Matrix':
        if isinstance(other, (int, float, numpy.ndarray)):
            return self.operate(operator_func = operator.add,
                                factor = other)
        elif isinstance(other, Matrix):
            return self.operate(operator_func = operator.add, 
                                matrix = other)
        else:
            return NotImplemented
        
    def __radd__(self, other) -> 'Matrix':
        return self.__add__(other)
    
    def __sub__(self, other) -> 'Matrix':
        if isinstance(other, (int, float, numpy.ndarray)):
            return self.operate(operator_func = operator.sub,
                                factor = other)
        elif isinstance(other, Matrix):
            return self.operate(operator_func = operator.sub, 
                                matrix = other)
        else:
            return NotImplemented
        
    def __rsub__(self, other) -> 'Matrix':
        if isinstance(other, (int, float, numpy.ndarray)):
            return self.operate(operator_func = operator.sub,
                                factor = other,
                                reverse = True)
        elif isinstance(other, Matrix):
            return self.operate(operator_func = operator.sub, 
                                matrix = other,
                                reverse = True)
        else:
            return NotImplemented
    
    def operate(self,
            operator_func,
            matrix: 'Matrix' = None,
            factor: float = None,
            new_level: int = -1,
            new_purpose: str = "",
            new_time_period: str = "",
            new_vehicle_type: str = "",
            reverse: bool = False) -> 'Matrix':
        '''creates a new matrix from the addition of the values of this matrix 
        and the other given matrix
        if factor provided instead of matrix, will carry out operation 
        with that value
        operator function will normally be a function from the operator library
        '''
        if factor is not None and matrix is not None:
            raise MatrixError("operate must not be provided factor and matrix")
        if (not isinstance(matrix, Matrix) and
            not isinstance(factor, (int, float, numpy.ndarray))):
            raise MatrixError(
                "operate needs a matrix as a matrix or a factor as an "
                "int/float/ndarray")
        if isinstance(matrix, Matrix):
            if (self.dim != matrix.dim and 
                self.zone_count != matrix.zone_count):
                raise MatrixError(
                    "operate given two matrices of different sizes")
            if reverse:
                new_matrix = operator_func(matrix.matrix, self.matrix)
            else:
                new_matrix = operator_func(self.matrix, matrix.matrix)
            comments = (f'Created by operation {operator_func} '
                        f'(reverse: {reverse}) on\n'
                        f'[\n{self.get_string()}\n]\n'
                        f'{operator_func}\n'
                        f'[\n{matrix.get_string()}\n]\n')
        elif isinstance(factor, (int, float, numpy.ndarray)):
            if (isinstance(factor, numpy.ndarray) and
                self.dim != factor.shape):
                raise MatrixError(
                    "operate given numpy array of a shape dissimilar "
                    "to the matrix shape")
            if reverse:
                new_matrix = operator_func(factor, self.matrix)
            else:
                new_matrix = operator_func(self.matrix, factor)
            comments = (f'Created by operation {operator_func} '
                        f'(reverse: {reverse}) on\n'
                        f'[\n{self.get_string()}\n]\n'
                        f'{operator_func}\n'
                        f'[\n{factor}\n]\n')
        output_mat = self._copy(
            level = new_level if new_level > 0 else self._level,
            purpose = new_purpose if new_purpose != "" else self._purpose,
            time_period = new_time_period if new_time_period != "" 
                            else self._time_period,
            vehicle_type = new_vehicle_type if new_vehicle_type != ""
                            else self._vehicle_type,
            new_comments = comments,
            matrix_array = new_matrix)
        return output_mat
    
    def zero_intrazonals(self) -> 'Matrix':
        '''replaces all values that are intrazonals with zero
        '''
        new_matrix = self.matrix
        numpy.fill_diagonal(new_matrix, 0)
        return self._copy(
            operation_for_comment = "zero_intrazonals",
            matrix_array = new_matrix)
    
    def copy(self) -> "Matrix":
        '''Returns a copy of the matrix, with a comment label 
        indicating where the copy was made from for future reference
        '''
        return self._copy(operation_for_comment = "copy")
    
    def _copy(self,
              zones: 'Zones' = None,
              level: int = None,
              purpose: str = None,
              time_period: str = None,
              vehicle_type: str = None,
              value_type: str = None,
              new_comments: str = None,
              operation_for_comment: str = None,
              matrix_array: 'numpy.ndarray' = None) -> 'Matrix':
        if operation_for_comment is not None:
            new_comments = (f"Created by {operation_for_comment} operation on "
                            f"the following matrix:\n"
                            f"[\n{self.get_string()}\n]\n")
        matrix_copy = Matrix(
            zones = self.zones_object if zones is None else zones, 
            level = self.level if level is None else level, 
            purpose = self.purpose if purpose is None else purpose,
            time_period = self.time_period if time_period is None 
                            else time_period, 
            vehicle_type = self.vehicle_type if vehicle_type is None 
                            else vehicle_type,
            value_type = self.value_type if value_type is None 
                            else value_type, 
            comments = self.comments if new_comments is None 
                            else new_comments, 
            matrix_array = self.matrix if matrix_array is None 
                            else matrix_array)
        return matrix_copy
    
    def set_cells(self,
                  values_dict: dict,
                  inplace = True
                  ):
        '''sets a cell or range of cells to be a certain value
        values_dict keys can be:
            row_{ref}
            col_{ref}
            {ref}_{ref}
        and values must be the value to overwrite with
        e.g.
            {'row_5': 1,
             'col_4': 0,
             '6_7': 0}
            would set all values in row 5 to 1, all values in column 4 to 0, 
            and pair (6, 7) to 0
        '''
        new_matrix = self.matrix
        for key, value in values_dict.items():
            if key[0:4] == 'row_':
                ref = key[4:]
                num = self._zones.get_position(ref)
                new_matrix[num] = value
            elif key[0:4] == 'col_':
                ref = key[4:]
                num = self._zones.get_position(ref)
                new_matrix[:, num] = value
            elif "_" in key:
                ref1, ref2 = key.split("_")
                num1 = self._zones.get_position(ref1)
                num2 = self._zones.get_position(ref2)
                new_matrix[num1, num2] = value
            else:
                raise MatrixError(
                    'set_cells: Invalid key type given in values_dict'
                    f'given {key}')
        if inplace:
            self._matrix = new_matrix
        else:
            return self._copy(matrix_array = new_matrix,
                              comments = 'Created by set_cells operation')
    
    def get_zones_summary(self) -> 'pandas.DataFrame':
        '''Get a summary of the zones and information about them 
        in tabular form
        '''
        return self._zones.summary(
                    additional_data = {
                    'o_total': self.row_totals,
                    'd_total': self.col_totals,
                    'intrazonal': self.intrazonals})
    
    def zone_sum(self, zone_reference: str
                 ) -> tuple:
        '''get the sum of the row and column for the given zone reference
        '''
        if zone_reference != 0:
            if zone_reference not in self.zones:
                raise MatrixError("zone_sum: zone reference not in zones list")
            zone_index = self._zones.get_index(zone_reference)
        return (numpy.sum(self._matrix[zone_index]), 
                numpy.sum(self._matrix[:, zone_index]))
    
    def import_matrix(self,
                      matrix_file: PurePath,
                      sheet_name: str = "",
                      origin_field: str = 'o_zone',
                      destination_field: str = 'd_zone',
                      values_field: str = 'trips',
                      queries: list = None,
                      output_workings_folder: PurePath = None
                      ) -> None:
        ''' imports matrix data from an external file in record format
        assumes fields of origin, destination, values
        queries allows queries to be run on the dataset
            e.g. f'period == "{time_period}"'
            see pandas doc for query construction
            https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.query.html
        '''
        framework_df = self._zones.create_odlist_dataframe(
            o_ref = origin_field,
            d_ref = destination_field)
        raw_df = dataframe_import(matrix_file, sheet_name)
        check_lists_equal(self.zones,
                          raw_df[origin_field].unique().tolist(),
                          'zones',
                          'origin',
                          'raw_df from import_matrix')
        check_lists_equal(self.zones,
                          raw_df[destination_field].unique().tolist(),
                          'zones',
                          'destination',
                          'raw_df from import_matrix')
        query_df = raw_df.copy()
        if isinstance(queries, list):
            for query_statement in queries:
                try:
                    query_df = query_df.query(query_statement)
                except:
                    print(query_statement)
                    print(query_df.columns)
                    raise
        trimmed_df = query_df.loc[:, [origin_field, 
                                      destination_field, 
                                      values_field]]
        matrix_df = framework_df.merge(trimmed_df,
                                    on = [origin_field, destination_field],
                                    how = 'outer')
        matrix_fill_df = matrix_df.fillna(0)
        pivoted_df = matrix_fill_df.groupby(
            [origin_field, destination_field]).sum().unstack()

        self._matrix = pivoted_df.to_numpy()
        self._comments = f'Imported matrix from {matrix_file}\n{self.comments}'
        if output_workings_folder != None:
            output_workings = output_workings_folder / f'{self.file_name}.xlsx'
            with pandas.ExcelWriter(output_workings,
                                    engine = 'openpyxl') as writer:
                framework_df.to_excel(writer, sheet_name = 'framework_df')
                raw_df.to_excel(writer, sheet_name = 'raw_df')
                query_df.to_excel(writer, sheet_name = 'query_df')
                trimmed_df.to_excel(writer, sheet_name = 'trimmed_df')
                matrix_df.to_excel(writer, sheet_name = 'matrix_df')
                matrix_fill_df.to_excel(writer, sheet_name = 'matrix_fill_df')
                pivoted_df.to_excel(writer, sheet_name = 'pivoted_df')

    def import_matrix_csv_square(self,
                             matrix_file: PurePath,
                             header_rows: int = 0,
                             header_cols: int = 1,
                             level_number: int = 1
                             ) -> None:
        '''imports a matrix from a square CSV matrix file 
        (assumes SATURN format by default, with a zone number per row)
        '''
        line_start = header_rows + len(self.zones) * (level_number - 1) + 1
        line_end = header_rows + len(self.zones) * level_number
        
        matrix_data = []
        with open(matrix_file, 'r') as import_file:
            line_count = 0
            for input_line in import_file:
                line_count += 1
                if line_count >= line_start and line_count <= line_end:
                    try:
                        input_line = input_line[:-1]
                        input_line = input_line.split(',')[header_cols:]
                        input_line = [float(item) for item in input_line]
                        if len(input_line) != len(self.zones):
                            raise MatrixError(
                                f"import_matrix_csv_square: matrix being "
                                f"read does not have the correct number of "
                                f"zones (number of cols ({len(input_line)}) "
                                f"not equal to zone list length "
                                f"({len(self.zones)})")
                        matrix_data.append(input_line)
                    except:
                        print(f"line_start/end: {line_start} {line_end}")
                        print(f"line_count: {line_count}")
                        print(f"matrix_file: {matrix_file}")
                        print(f"line_count: {input_line}")
                        raise
        self._matrix = numpy.array(matrix_data)
        self._comments = (f'Imported matrix from {matrix_file}\n'
                          f'{self.get_string()}')
        return self
                
    def output_summary(self,
                       working_dir: PurePath,
                       file_prefix: str = "") -> str:
        '''dumps a csv summary of the matrix
        '''
        if working_dir.exists == False:
            raise MatrixError(
                f'output_summary could not find directory: {working_dir}')
            
        output_file_name = ''
        if file_prefix != '':
            output_file_name = f'{file_prefix}_'
        output_file_name = f'{output_file_name}{self.file_name}_summary.csv'
        output_file_location = working_dir / output_file_name
        with open(output_file_location, 'w') as output_summary:
            output_summary.write(f'{self.get_string()}')
        self.get_zones_summary().to_csv(output_file_location,
                                        mode = 'a')
        return output_file_location
        
    def save(self, 
             output_directory: PurePath,
             prefix: str = "") -> str:
        '''saves the matrix object to a json file
        jsonpickle automatically converts the object to a json representation
        use load_matrix(file_path) to restore the object from a file
        '''
        if prefix != "":
            output_file_name = (f"{prefix}_{self.file_name}.json")
        else:
            output_file_name = (f"{self.file_name}.json")
        output_file_location = output_directory / output_file_name
        with open(output_file_location, 'w') as save_file:
            save_file.write(jsonpickle.encode(self,
                                              keys = True))
        return output_file_location
    
    def save_csv(self,
                    output_directory: PurePath,
                    prefix: str = "",
                    precision: int = 2) -> str:
        '''saves the matrix information to a csv
        '''
        if prefix != "":
            output_file_name = f'{prefix}_{self.file_name}_values.csv'
        else:
            output_file_name = f'{self.file_name}_values.csv'
        output_file_location = output_directory / output_file_name
        output_matrix = self.matrix_for_csv
        output_matrix.to_csv(output_file_location,
                             float_format = f"%.{precision}f",
                             header = False)
        return output_file_location
    
    def disaggregate_matrix(self, 
                            origin_map_name: str,
                            destination_map_name: str,
                            new_zone_system: 'Zones',
                            output_checking_folder: Path = None
                            ) -> 'Matrix':
        '''uses mappings of current zone system -> new zone system with 
        proportions to rebuild the matrix for the new zone system
        
        origin_map_name and destination_map_name must be the name of a 
            disaggregation map in the zone system associated with this matrix
        '''
        
        MESSAGE_HOOK(f'{time.ctime(time.time())} disaggregate matrix: '
                     f'{origin_map_name}, {destination_map_name}')
        if origin_map_name not in self.zones_object.disaggregation_maps.keys():
            raise MatrixError(
                f"disaggregate_matrix origin map name doesn't "
                f"exist for zone system ({origin_map_name}, "
                f"{self.zones_object.disaggregation_maps.keys()})")
        if (destination_map_name not in 
            self.zones_object.disaggregation_maps.keys()):
            raise MatrixError(
                f"disaggregate_matrix destination map name doesn't "
                f"exist for zone system ({destination_map_name}, "
                f"{self.zones_object.disaggregation_maps.keys()})")
        
        old_zones = self.zones
        new_zones = new_zone_system.references
        new_matrix = numpy.zeros((len(new_zones), len(new_zones)))
        origin_map = self.zones_object.disaggregation_maps[origin_map_name]
        dest_map = self.zones_object.disaggregation_maps[destination_map_name]
        
        for disagg_name, disagg_map in [
                (origin_map_name, origin_map),
                (destination_map_name, dest_map)]:
            unique_zones = set()
            for zone_map in disagg_map.values():
                unique_zones.update(zone_map.targets.keys())
            check_lists_equal(new_zones, list(unique_zones), 
                              'new_zones', 
                              f'disaggregation_map: {disagg_name}',
                              'Matrix disaggregate_matrix')
        
        # mappings are converted into dataframes to allow merging to the 
        # record-format (melted) matrix later on
        origin_mergeframe = pandas.DataFrame.from_dict(
            {zone: mapping.targets for zone, mapping in origin_map.items()},
            orient='index').stack().to_frame().reset_index()
        origin_mergeframe.rename(
            columns={'level_0': 'origin', 
                     'level_1': 'origin_target',
                     0: 'origin_prop'}, 
            inplace=True)
        dest_mergeframe = pandas.DataFrame.from_dict(
            {zone: mapping.targets for zone, mapping in dest_map.items()},
            orient='index').stack().to_frame().reset_index()
        dest_mergeframe.rename(
            columns={'level_0': 'dest', 
                     'level_1': 'dest_target',
                     0: 'dest_prop'}, 
            inplace=True)

        # matrix converted into pandas record-format (melted) dataframe
        # to allow merge operation
        pandas_matrix = pandas.DataFrame(self._matrix,
                                         index=old_zones,
                                         columns=old_zones)
        pandas_matrix.reset_index(inplace=True)
        pandas_matrix.rename(columns={'index': 'origin'}, inplace=True)
        matrix_records = pandas.melt(
            pandas_matrix,
            id_vars='origin',
            var_name='dest',
            value_name=self.value_type)

        # merge the matrix with the proportion mappings and calculate
        # final matrix values
        merged_df = (
            matrix_records.merge(origin_mergeframe).merge(dest_mergeframe))
        merged_df['factor'] = (
            merged_df['origin_prop'] * merged_df['dest_prop'])
        merged_df['new_value'] = (
            merged_df[self.value_type] * merged_df['factor'])
        final_df = merged_df.drop(
            columns=[column for column in merged_df.columns 
                     if column not in 
                     ['origin_target', 'dest_target', 'new_value']])
        # this step is the equivalent of using pivot, but it deals with 
        # duplicate values which pivot does not
        output_matrix_df = final_df.groupby(
            ['origin_target', 'dest_target']).sum().unstack()
        new_matrix = output_matrix_df.to_numpy()

        if output_checking_folder is not None:
            output_data = {'origin_mergeframe': origin_mergeframe,
                 'dest_mergeframe': dest_mergeframe,
                 'pandas_matrix': pandas_matrix,
                 'matrix_records': matrix_records,
                 'merged_df': merged_df,
                 'final_df': final_df,
                 'output_matrix_df': output_matrix_df}
            # csv format used because dumping to excel with 
            # to_excel (dump_dataframes_to_workbook) is very VERY slow
            for filename, output in output_data.items():
                output.to_csv(output_checking_folder / 
                              f"{self.file_name}_{filename}.csv")
            numpy.savetxt(output_checking_folder / 
                          f"{self.file_name}_new_matrix.csv",
                          new_matrix,
                          delimiter=",",
                          fmt='%.3f')
            
        return self._copy(zones = new_zone_system,
                      operation_for_comment = "disaggregate_matrix",
                      matrix_array = new_matrix)
    
    def redistribute_matrix(self,
                            redist_map_name: str,
                            trip_end: str,
                            distance_filter: float = None
                            ) -> ('Matrix', 'Matrix'):
        '''modifies trip ends within the matrix using a disaggregation map
        returns two matrices, one of the redistributed matrix and
        one of the remainder trips that have been redistributed.
        
        trip end must be 'origin' or 'destination'
        distance filter is used to limit whether the redistribution is only
        used on trips longer than a given length
        
        for use in rail redistribution or other similar circumstances where
        trip ends might want to be moved.
        e.g. rail trips are from o to d and not from station to station
        redist would be run on outbound trips for origins to move origins
        to station locations, but only where access is longer than walk
        access distance.
        '''
        if trip_end not in ['origin', 'destination']:
            raise MatrixError(
                f"trip end must be one of 'origin', 'destination', "
                f"given {trip_end}")
        redist_map = self.zones_object.disaggregation_maps[redist_map_name]
        unique_zones = set()
        for zone_map in redist_map.values():
            unique_zones.update(zone_map.targets.keys())
        print(f"redistribute matrix unique_zones: {unique_zones}") 
        print(f"redistribute matrix self.zones: {self.zones}") 
        check_lists_equal(self.zones, list(unique_zones), 
                          'zones', 
                          f'redistribution map: {redist_map_name}',
                          'Matrix redistribute_matrix')
        # NOTE:
        # the first time this process is run it modifies the base zonemapping 
        # information, so then on subsequent passes it will already
        # have the distance filtering applied to the zonemapping objects
        if distance_filter is not None:
            for zone, mapping in redist_map.items():
                for target in mapping.target_zones:
                    distance = self.zones_object.get_distance(
                        zone, target)
                    if distance < distance_filter:
                        redist_map[zone].remove_target(target)
            # can be unintended consequences if all targets are removed
            # so if no targets remain we need to re-add a target of itself
            # also need to reproportion to make sure things add up
            count1 = 0
            count2 = 0
            for zone, mapping in redist_map.items():
                if mapping.count == 0:
                    mapping.add_target(zone, 1)
                    count1 += 1
                else:
                    count2 += 1
                mapping = mapping.proportions()
        
        remainder_matrix = self.zones_object.create_zero_matrix()
        redistributed_matrix = self.matrix
        for zone, mapping in redist_map.items():
            zone_idx = self.zones.index(zone)
            for target, proportion in mapping.targets.items():
                target_idx = self.zones.index(target)
                if trip_end == 'origin':
                    redistributed_matrix[target_idx, :] += (
                        self.matrix[zone_idx, :] * proportion)
                    redistributed_matrix[zone_idx, :] -= (
                        self.matrix[zone_idx, :] * proportion)
                    if zone_idx != target_idx:
                        remainder_matrix[zone_idx, target_idx] += (
                            self.matrix[zone_idx, :].sum() * proportion)
                elif trip_end == 'destination':
                    redistributed_matrix[:, target_idx] += (
                        self.matrix[:, zone_idx] * proportion)
                    redistributed_matrix[:, zone_idx] -= (
                        self.matrix[:, zone_idx] * proportion)
                    if zone_idx != target_idx:
                        remainder_matrix[target_idx, zone_idx] += (
                            self.matrix[:, zone_idx].sum() * proportion)
        redist_matrix = self._copy(
            operation_for_comment = 
                "redistribute_matrix (redistributed matrix)",
            matrix_array = redistributed_matrix)
        remain_matrix = self._copy(
            operation_for_comment = "redistribute_matrix (remaining matrix)",
            matrix_array = remainder_matrix)
        return redist_matrix, remain_matrix
    
    def zone_field_tlds(self,
                    field_name: str,
                    brackets: list,
                    distances: 'Matrix' = None,
                    proportions: bool = False,
                    intrazonals_bracket: bool = True,
                    return_format: str = 'dict'):
        """creates a set of TLDs for trips going in to, out of, and within
        each value in the given zone field. 
        
        Originally intended for generating per-sector breakdowns of 
        trip length distributions
        
        field_name is the name of the field in the zones df to use
        brackets is the distribution brackets
        distances is the values to use for determining which bucket the 
            resultant value sits within
        if distance matrix given is None, generates distance matrix from zones
        if proportions is set to true, generates proportions instead of values
        if intrazonals_bracket is set to true, generates a 0-1m bracket
        return format can be 'dict' or 'DataFrame' and will return the data
            in that format
        """
        field_values = sorted(self.zones_object.zone_df[field_name].unique())
        if return_format == 'dict':
            distributions = {}
        elif return_format == 'DataFrame':
            distributions = None
        for value in field_values:
            internal = [value]
            external = [item for item in field_values if item != value]
            internal_only = self.zones_object.create_filter_matrix(
                field_name, internal, internal)
            external_internal = self.zones_object.create_filter_matrix(
                field_name, external, internal)
            internal_external = self.zones_object.create_filter_matrix(
                field_name, internal, external)
            for filter_name, filter_matrix in {
                    'int-int': internal_only, 
                    'ext-int': external_internal, 
                    'int-ext': internal_external}.items():
                result = self._copy(matrix_array = self.matrix * filter_matrix)
                tld = result.get_tld(
                        brackets,
                        distances,
                        proportions,
                        intrazonals_bracket,
                        return_format,
                        f'{value}_{filter_name}')
                if return_format == 'dict':
                    distributions[f'{value}_{filter_name}'] = tld
                elif return_format == 'DataFrame':
                    if distributions is None:
                        distributions = tld.copy()
                    else:
                        distributions = distributions.join(tld)
        return distributions
    
    def get_tld(self,
                brackets: list,
                distances: 'Matrix' = None,
                proportions: bool = False,
                intrazonals_bracket: bool = True,
                return_format: str = 'dict',
                header_suffix: str = '') -> dict:
        '''generates a dictionary containing trip lengths and values
        if distance matrix given is None, generates distance matrix from zones
        if proportions is set to true, generates proportions instead of values
        if intrazonals_bracket is set to true, generates a 0-1m bracket
        return format can be 'dict' or 'DataFrame' and will return the data
            in that format
        '''
        if distances is None:
            distances = self.zones_object.create_distance_matrix().matrix
        elif isinstance(distances, Matrix):
            distances = distances.matrix
        if not isinstance(distances, numpy.ndarray):
            raise MatrixError(
                f"get_tld expected distances as array, "
                f"given {type(distances)}")
        if not isinstance(brackets[0], int):
            raise MatrixError(f"get_tld expected bracket as list of ints, "
                              f"given type: {type(brackets[0])}, "
                              f"brackets: {brackets}")
        
        if brackets[0] == 0:
                brackets.pop(0)
        if intrazonals_bracket:
            bands = {(0, 1): 0}
        else:
            bands = {}
        for bracket_num in range(len(brackets)):
            if intrazonals_bracket and len(bands) == 1:
                bands[(1, brackets[bracket_num])] = 0
            elif not intrazonals_bracket and len(bands) == 0:
                bands[(0, brackets[bracket_num])] = 0
            else:
                bands[(brackets[bracket_num - 1],
                       brackets[bracket_num])] = 0
        bands[(brackets[-1], 1000000000)] = 0
        
        trips = self.matrix
        for band in bands.keys():
            matching_cells = ((distances >= band[0]) *
                              (distances < band[1]))
            bands[band] = (matching_cells * trips).sum()
        
        if proportions:
            prop_bands = {}
            sum_total = sum(bands.values())
            for key, value in bands.items():
                prop_bands[key] = value / sum_total
            bands = prop_bands
        
        if return_format == 'dict':
            return bands
        elif return_format == 'DataFrame':
            df_data = {}
            df_data['brackets'] = list(bands.keys())
            if header_suffix == "":
                new_col = self.purpose
            else:
                new_col = f'{self.purpose}_{header_suffix}'
            df_data[new_col] = list(bands.values())
            tld_df = pandas.DataFrame(df_data)
            tld_df = tld_df.set_index('brackets')
            return tld_df
        else:
            raise MatrixError(
                f'get_tld given unknown return_format: {return_format}')
    
    def operate_by_property(self,
                               bands_values: dict,
                               operator_func,
                               bandvalue_matrix: 'Matrix' = None,
                               reverse: bool = False
                               ) -> 'Matrix':
        '''returns a new matrix populated with values based on the 
        bands_values dictionary
        bands are a tuple of (value1, value2)
        uses bandvalue_matrix values to determine what value to populate
        e.g. if band (0, 5) is given with a value of 10, and an operator of
        operator.add is provided then it will add 10 to
        all values in the positions matching those with a value  
        0 <= x < 5 in the bandvalue_matrix.
        
        can be used for things such as applying factors based on distances
        '''
        if bandvalue_matrix is None:
            bandvalue_matrix = self.matrix
        if isinstance(bandvalue_matrix, Matrix):
            bandvalue_matrix = bandvalue_matrix.matrix
        current_matrix = self.matrix
        operator_matrix = []
        for band, value in bands_values.items():
            matching_cells = ((bandvalue_matrix >= band[0]) *
                              (bandvalue_matrix < band[1]))
            operator_matrix.append(matching_cells * value)
        operator_matrix = sum(operator_matrix)
        if reverse:
            operated_matrix = operator_func(operator_matrix, current_matrix)
        else:
            operated_matrix = operator_func(current_matrix, operator_matrix)
        return self._copy(
            operation_for_comment = f"operate_by_property ({operator_func})",
            matrix_array = operated_matrix)
    
    def generate_proportion_matrix(self,
                                   other: 'Matrix',
                                   max_value: float = None,
                                   check_above_one: bool = True,
                                   output_folder: 'PurePath' = None
                                   ) -> 'Matrix':
        '''returns a new matrix with proportions instead of values
        proportions are generated per-cell as a proportion of the other matrix
        '''
        if not isinstance(other, Matrix):
            raise MatrixError(
                f"generate_proportion_matrix expects other to be a "
                f"Matrix object (given {type(other)})")
        proportions = self.matrix / other.matrix
        proportions = numpy.nan_to_num(proportions)
        if max_value is not None:
            proportions[proportions > max_value] = max_value
        if output_folder is not None:
            proportions_matrix = self._copy(matrix_array = proportions)
            proportions_matrix.save_csv(output_folder, 'proportions_check')
        if check_above_one:
            count = (proportions > 1).sum()
            if count > 0:
                raise MatrixError(
                    f"generate_proportion_matrix for {self.short_name} "
                    f"found {count} proportions above 1 (out of {self.size})\n"
                    f"average proportion = {proportions.mean()}\nmax = "
                    f"{proportions.max()}, min = {proportions.min()}")
        comments = (f'Created by generate_proportion_matrix operation on '
                    f'the following matrices:\n'
                    f'[\n{self.get_string()}\n]\n'
                    f'[\n{other.get_string()}\n]\n')
        if self.purpose != other.purpose:
            new_purpose = f'{self.purpose}_vs_{other.purpose}'
        else:
            new_purpose = self.purpose
        return self._copy(value_type = PROP_STR,
                          matrix_array = proportions,
                          purpose = new_purpose,
                          new_comments = comments)
    
    def furness_matrix(self,
                       row_factors: list,
                       col_factors: list,
                       max_iterations: int = 10,
                       convergence_criteria: float = 0.01
                       ) -> 'Matrix':
        '''runs a furness process on the matrix
        targets are given by origin / destination factors multiplied by 
        current row/column totals
        '''
        if (len(row_factors) != self.zone_count or 
            len(col_factors) != self.zone_count):
            raise MatrixError(
                f'furness_process factor lists need to be equal in length '
                f'to the number of zones in the matrix '
                f'(given {len(row_factors)} and {len(col_factors)} '
                f'against {self.zone_count})')
        convergence_targets = (1 - convergence_criteria, 
                               1 + convergence_criteria)
        row_totals = self.row_totals
        col_totals = self.col_totals
        furness_matrix = self.matrix
        
        row_targets = []
        col_targets = []
        for index in range(self.zone_count):
            row_targets.append(row_totals[index] * row_factors[index])
            col_targets.append(col_totals[index] * col_factors[index])
        row_target_total = sum(row_targets)
        col_target_total = sum(col_targets)
        #need to rescale targets if not equal to make furness process converge
        if row_target_total != col_target_total:
            divisor = (row_target_total + col_target_total) / 2
            row_scale = col_target_total / divisor
            col_scale = row_target_total / divisor
            row_targets = [row_target * row_scale for 
                           row_target in row_targets]
            col_targets = [col_target * col_scale for
                           col_target in col_targets]
        
        #row has col test and col has row test
        iteration_record = []
        for iteration in range(max_iterations):
            for index in range(self.zone_count):
                target = row_targets[index]
                current = row_totals[index]
                if current == 0:
                    scale_factor = 1
                else:
                    scale_factor = target / current
                furness_matrix[index] *= scale_factor
            col_totals = furness_matrix.sum(axis=0)
            col_pass = 0
            for index in range(self.zone_count):
                if col_totals[index] > 0 and col_targets[index] > 0:
                    convergence = col_totals[index] / col_targets[index]
                    if (convergence >= convergence_targets[0] and 
                        convergence <= convergence_targets[1]):
                        col_pass += 1
                else:
                    col_pass += 1
            
            for index in range(self.zone_count):
                target = col_targets[index]
                current = col_totals[index]
                if current == 0:
                    scale_factor = 1
                else:
                    scale_factor = target / current
                furness_matrix[:,index] *= scale_factor
            row_totals = furness_matrix.sum(axis=1)
            row_pass = 0
            for index in range(self.zone_count):
                if row_totals[index] > 0 and row_targets[index] > 0:
                    convergence = row_totals[index] / row_targets[index]
                    if (convergence >= convergence_targets[0] and 
                        convergence <= convergence_targets[1]):
                        row_pass += 1
                else:
                    row_pass += 1
            
            iteration_record.append(
                f'{iteration + 1}: count: {self.zone_count}, '
                f'col_pass: {col_pass}, row_pass: {row_pass}')
            if row_pass == self.zone_count:
                break
            
        MESSAGE_HOOK(f"furness_matrix finished convergence / hit max "
                     f"iterations after iteration {iteration + 1}\n"
                     f"{iteration_record[-1]}")
        iteration_string = "Convergence Information:\n"
        for record in iteration_record:
            iteration_string = f"{iteration_string}{record}\n"
        return self._copy(
            new_comments = f"Created by furness_matrix operation on the "
                            f"following matrix:\n"
                            f"[\n{self.get_string()}\n]\n"
                            f"{iteration_string}",
            matrix_array = furness_matrix)
    
    def aggregate_by_zone_field(self, 
                                zone_field_name: str,
                                new_zone_system: 'Zones' = None,
                                calc_type: str = 'sum',
                                retention: dict = None
                                ) -> 'Matrix':
        '''aggregates the matrix by the zone field given
        e.g. for sectoring, provide sector list in zones object
        then specify 'sector' as zone field name and provide a new 
        zones object based on sectors
        '''
        if new_zone_system is None:
            new_zone_system = self.zones_object.summarise_by_field(
                field_name = zone_field_name,
                retention = retention)
        current_matrix = self.matrix
        if zone_field_name not in self.zones_object.columns:
            raise MatrixError(
                'aggregate_by_zone_field: field not in zones object')
        zone_df = self.zones_object.zone_df
        field_values = zone_df[zone_field_name].unique()
        check_lists_equal(field_values, 
                          new_zone_system.references,
                          'old_zone_system_info',
                          'new_zone_system_info',
                          'aggregate_by_zone_field')
        lookup_list = list(zone_df[zone_field_name])
        origin, dest = numpy.meshgrid(lookup_list, lookup_list)
        
        new_reference = new_zone_system.references
        if calc_type == 'sum':
            new_matrix = numpy.zeros([len(new_reference), len(new_reference)])
            for row in range(len(current_matrix)):
                new_row = new_reference.index(lookup_list[row])
                for col in range(len(current_matrix)):
                    new_col = new_reference.index(lookup_list[col])
                    new_matrix[new_row, new_col] += current_matrix[row, col]
        elif calc_type == 'average':
            sum_matrix = numpy.zeros([len(new_reference), len(new_reference)])
            count_matrix = numpy.zeros([len(new_reference), 
                                        len(new_reference)])
            new_matrix = numpy.zeros([len(new_reference), len(new_reference)])
            for row in range(len(current_matrix)):
                new_row = new_reference.index(lookup_list[row])
                for col in range(len(current_matrix)):
                    new_col = new_reference.index(lookup_list[col])
                    sum_matrix[new_row, new_col] += current_matrix[row, col]
                    count_matrix[new_row, new_col] += 1
            new_matrix = (sum_matrix / count_matrix)
        else:
            raise MatrixError(
                f"aggregate_by_zone_field expected 'sum' or 'average' "
                f"for calc_type, given {calc_type}")
        
        # TODO may be a better way of doing this still
        # feels like this is more numpy-esque but is actually slower in practice?
        # leaving here for future reference
        # for row in range(len(new_reference)):
        #     row_matrix = origin == new_reference[row]
        #     for col in range(len(new_reference)):
        #         col_matrix = dest == new_reference[col]
        #         paired = row_matrix * col_matrix
        #         temp_matrix = (paired * current_matrix)
        #         new_matrix[row, col] = temp_matrix.sum()
        return self._copy(zones = new_zone_system,
                          operation_for_comment = 
                              f"aggregate_by_zone_field ({zone_field_name})",
                          matrix_array = new_matrix)
    
    
@standard_tools
def load_matrix(json_path: PurePath) -> 'Matrix':
    if json_path.exists == False:
        raise ValueError(f'load_matrix: file path not found ({json_path})')
    with open(json_path, 'r') as input_json:
        data = jsonpickle.decode(input_json.read(),
                                 keys = True)
    return data

def dataframe_import(file_name: PurePath,
                     sheet_name: str = ""):
    """imports data from a csv or excel sheet into a pandas dataframe
    """
    if not file_name.exists():
        raise ValueError(f"dataframe_import: file given does not exist: "
                         f"{file_name}")
    if file_name.suffix == '.csv':
        df = pandas.read_csv(file_name)
    elif file_name.suffix[:4] == '.xls':
        if sheet_name == "":
            raise ValueError("dataframe_import: excel file extension requires "
                             "sheet name to be specified")
        df = pandas.read_excel(file_name, sheet_name = sheet_name)
    else:
        raise ValueError(f'dataframe_import: File type given not recognised '
                         f'as valid; provide Excel or csv table '
                         f'(given {file_name.suffix})')
    return df

def dump_dataframes_to_workbook(workbook_name: str, 
                                datasets: dict,
                                save_dir: PurePath
                                ) -> None:
    """Dumps a given set of dataframes (contained in a nested dictionary)
    to a single excel workbook, with one worksheet per dataframe
    """
    with pandas.ExcelWriter(save_dir / f'{workbook_name}.xlsx') as excel_file:
        for dataset_name, dataset in recursive_dict_search(datasets):
            if len(dataset_name) > 31:
                print(f'{dataset_name} more than 31 chars in length, '
                      f'truncating to {dataset_name[:31]}')
                sheet_name = dataset_name[:31]
            else:
                sheet_name = dataset_name
            dataset.to_excel(excel_file, sheet_name=f'{sheet_name}')
            
def recursive_dict_search(dictionary: dict ,
                          start_string: str = ""):
    """iterator to search through a dictionary and return all
    non-dictionary entries, with a string identifying their path within
    the dictionary
    Use with a for loop to iterate through the nested dictionary
    """
    for key, value in dictionary.items():
        if start_string != "":
            keystring = f'{start_string}_{key}'
        else: 
            keystring = str(key)
        if isinstance(value, dict):
            yield from recursive_dict_search(value, 
                                             keystring)
        else:
            yield keystring, value

def check_lists_equal(list1: list,
                      list2: list,
                      list1_name: str,
                      list2_name: str,
                      error_from: str,
                      nan_ok: bool = False) -> None:
    '''compares two lists and returns new lists of those items missing from
    the other
    '''
    list1_missing = []
    list2_missing = []
    for item in list1:
        if item not in list2:
            list2_missing.append(item)
    for item in list2:
        if item not in list1:
            list1_missing.append(item)
    if nan_ok:
        if numpy.nan in list1_missing:
            list1_missing.remove(numpy.nan)
        if numpy.nan in list2_missing:
            list2_missing.remove(numpy.nan)
    if len(list1_missing) > 0 and len(list2_missing) > 0:
        raise ValueError(
            f"check_lists_equal (from {error_from}):\n{list1_name} missing "
            f"{list1_missing}\n{list2_name} missing {list2_missing}")
    elif len(list1_missing) > 0:
        raise ValueError(f"check_lists_equal (from {error_from}:\n"
                         f"{list1_name} missing {list1_missing}")
    elif len(list2_missing) > 0:
        MESSAGE_HOOK(f"WARNING: check_lists_equal (from {error_from}:\n"
                     f"{list2_name} missing {list2_missing}")
    
#---TESTING

ZONE_TEST_CSV = Path(r"C:\Projects\Repos\matrix_tools\test_data\zones.csv")
ZONE_TEST_OVERRIDE_CSV = Path(r"C:\Projects\Repos\matrix_tools\test_data"
                              r"\zones_overrides.csv")
ZONE_TEST_EXCEL = Path(r"C:\Projects\Repos\matrix_tools\test_data\zones.xlsx")
OUTPUT_TEST_SUMMARY = Path("C:/Projects/Repos/matrix_tools/test_data/")
TEST_ARRAY = numpy.array([[1.1,2.2,3.3],[2,3,4],[4,5,6]])
TEST_REDIST = pandas.DataFrame({'from_zone': [1], 
                                'to_zone': [2], 
                                'proportion': [1]})
TEST_REDIST_2 = pandas.DataFrame(
    {'from_zone':[1,1,2,2],
     'to_zone':[2,3,1,3],
     'proportion':[.5,.5,.5,.5]})

def stack_test(zone_file, zone_sheet = ""):
    new_zones = Zones("test zone system", 
                      zone_file, 
                      zone_sheet, 
                      purpose='test')
    new_stack = MatrixStack('test',
                            new_zones,
                            'temp',
                            'road_test',
                            'Trips')
    level_1 = Matrix(new_zones, 
                     purpose = 'HBW', 
                     time_period = 'temp', 
                     vehicle_type = 'Car',
                     matrix_array = TEST_ARRAY)
    level_2 = Matrix(new_zones, 
                     purpose = 'HBO', 
                     time_period = 'temp', 
                     vehicle_type = 'Car',
                     matrix_array = TEST_ARRAY)
    new_stack.add_matrix(1, level_1)
    new_stack.add_matrix(2, level_2)
    print(new_stack)

def zones_test(zone_file, zone_sheet = ""):
    new_zones = Zones("test zone system", zone_file, zone_sheet, 
                      purpose='test', test_metadata='test metadata')
    MESSAGE_HOOK(f"zone string:\n{new_zones}")
    MESSAGE_HOOK(f"zone references:\n{new_zones.references}\n")
    for zone in new_zones.references:
        MESSAGE_HOOK(f"zone details:\n{new_zones.get_zone_details(zone)}\n")
    zones_copy = new_zones.copy()
    MESSAGE_HOOK(zones_copy)
    MESSAGE_HOOK(zones_copy.metadata)
    MESSAGE_HOOK(new_zones.create_distance_matrix())
    
def zone_override_test(zones_file):    
    zones = Zones('test_overrides',
                  zones_file = zones_file,
                  column_overrides = {
                      REFERENCE: 'zone_id',
                      X_COORD: 'x',
                      Y_COORD: 'y'})
    MESSAGE_HOOK(zones)
    return zones

def matrix_test(zone_file, zone_sheet = ""):
    test_matrix = Matrix(Zones("test zone system", zone_file, zone_sheet), 
                         purpose = 'HBW', time_period = 'test', 
                         vehicle_type = 'Car',
                         matrix_array = TEST_ARRAY)
    MESSAGE_HOOK(f"Test matrix:\n{test_matrix}")
    copy_matrix = test_matrix.copy()
    MESSAGE_HOOK(f"Copy of Test matrix:\n{copy_matrix}")
    MESSAGE_HOOK(copy_matrix.get_zones_summary())
    copy_matrix.output_summary(OUTPUT_TEST_SUMMARY, 'test')
    copy_matrix.save_csv(OUTPUT_TEST_SUMMARY, 'test')
    MESSAGE_HOOK(copy_matrix.get_tld([0, 500, 1000, 1500]))
    return copy_matrix.save(OUTPUT_TEST_SUMMARY, 'test')
    
def matrix_add_test(zone_file, zone_sheet = ""):
    new_zones = Zones("test zone system", 
                      zone_file, 
                      zone_sheet, 
                      purpose='test')
    new_matrix = Matrix(new_zones)
    add_matrix = Matrix(new_zones,
                        matrix_array = TEST_ARRAY)
    added_matrix = new_matrix.add(add_matrix)
    MESSAGE_HOOK(f'add matrix: {add_matrix}')
    MESSAGE_HOOK(f'addition matrix: {added_matrix}')
    
def matrix_redist_test(zone_file, zone_sheet = "", 
                       redist_map = TEST_REDIST, direction = 'origin'):
    test_matrix = Matrix(Zones("test zone system", zone_file, zone_sheet), 
                         purpose = 'blah', time_period = 'test', 
                         vehicle_type = 'rail',
                         matrix_array = TEST_ARRAY)
    test_matrix._zones.add_disaggregation_map('redist',
                                              redist_map)
    redist_matrix, remain_matrix = test_matrix.redistribute_matrix(
        'redist',
        direction)
    print(test_matrix)
    print(redist_matrix)
    print(remain_matrix)
    print(test_matrix.matrix)
    print(redist_matrix.matrix)
    print(remain_matrix.matrix)
    
def matrixstack_redist_test(zone_file, zone_sheet = "", 
                       redist_map = TEST_REDIST):
    new_zones = Zones("test zone system", zone_file, zone_sheet)
    new_zones.add_disaggregation_map('redist',
                                     redist_map)
    test_stack = MatrixStack('test',
                            new_zones,
                            'temp',
                            'road_test',
                            'Trips')
    test_matrix = Matrix(zones = new_zones, 
                         purpose = 'blah', time_period = 'temp', 
                         vehicle_type = 'rail',
                         matrix_array = TEST_ARRAY)
    test_matrix2 = test_matrix.copy()
    test_matrix2.purpose = 'blah2'
    test_stack.add_matrix(1, test_matrix)
    test_stack.add_matrix(2, test_matrix2)
    redist_matrices, remain_matrices = test_stack.redistribute(
        'redist',
        {'blah': 'origin',
         'blah2': 'destination'})
    print(test_stack)
    print(redist_matrices)
    print(remain_matrices)
    print(test_stack.get_by_purpose('blah').matrix)
    print(redist_matrices.get_by_purpose('blah').matrix)
    print(remain_matrices.get_by_purpose('blah').matrix)
    print(test_stack.get_by_purpose('blah2').matrix)
    print(redist_matrices.get_by_purpose('blah2').matrix)
    print(remain_matrices.get_by_purpose('blah2').matrix)
    
    
def matrix_load_test(file_path):
    loaded_matrix = load_matrix(file_path)
    print(loaded_matrix)
    return loaded_matrix

if __name__ == "__main__":
    pass
    ZONE_FILE = ZONE_TEST_EXCEL
    ZONE_SHEET = 'zones'
    # matrices = stack_test(ZONE_FILE, ZONE_SHEET)
    # zones_test(ZONE_FILE, ZONE_SHEET)
    # zone_override_test(ZONE_TEST_OVERRIDE_CSV)
    matrix_test(ZONE_FILE, ZONE_SHEET)
    # matrix_load_test(matrix_test(ZONE_FILE, ZONE_SHEET))
    # matrix_add_test(ZONE_FILE, ZONE_SHEET)
    # matrix_redist_test(ZONE_FILE, ZONE_SHEET,TEST_REDIST_2)
    # matrixstack_redist_test(ZONE_FILE, ZONE_SHEET, TEST_REDIST_2)
                      