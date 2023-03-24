# -*- coding: utf-8 -*-

import functools
import time
import pandas

#Redefines print in case method of display changes from simple print to more complicated function
MESSAGE_HOOK = print

def standard_tools(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        #Sets display options for pandas dataframes while using function and displays time before and after performing function
        with pandas.option_context("display.min_rows", 3,
                                   "display.precision", 1):
            MESSAGE_HOOK(f'{time.ctime(time.time())}: {func.__name__} run using arguments: {args}, {kwargs}')
        func_return = func(*args, **kwargs)
        MESSAGE_HOOK(f'{time.ctime(time.time())}: {func.__name__} completed')
        return func_return
    return wrapper


## see: https://www.codementor.io/@sheena/advanced-use-python-decorators-class-function-du107nxsv
def class_standard_tools(Cls):
    class TempCls(object):
        def __init__(self, *args, **kwargs):
            self.oInstance = Cls(*args, **kwargs)
            
        def __getattribute__(self, attribute):
            """
            this is called whenever any attribute of a TempCls object is 
            accessed. This function first tries to get the attribute 
            of TempCls. If it fails then it tries to fetch the attribute 
            from self.oInstance (an instance of the decorated class). 
            If it manages to fetch the attribute from self.oInstance, and 
            the attribute is an instance method then standard_tools is applied.
            """
            try:
                temp_instance_1 = super(TempCls,self).__getattribute__(attribute)
            except AttributeError:
                pass
            else:
                return temp_instance_1
            temp_instance = self.oInstance.__getattribute__(attribute)
            if isinstance(temp_instance, self.__init__):
                return standard_tools(temp_instance)
            else:
                return temp_instance
    return TempCls