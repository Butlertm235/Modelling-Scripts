"""
Created September 2024

@author: UKSAC775
"""

import os

def get_filepath_this_script(file):

    """
    Return the file path of the python script that calls this function, where '__file__' is specified as the 'file' argument
    
    """

    filepath = os.path.dirname(os.path.abspath(file)).replace("\\","/")+"/"
    return filepath