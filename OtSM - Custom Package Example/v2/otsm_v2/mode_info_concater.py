"""
Author: Thomas Butler

Contributors:

Last Updated: 12/09/2023
"""

from pathlib import Path
import pandas as pd
import logging
import os

#INPUTS
INPUT_SHORT_FILE_NAME = 'short_mode_info.csv'
INPUT_LONG_FILE_NAME = 'long_mode_info.csv'

PROGRAM_DIRECTORY = os.getcwd()
OUTPUTS_DIRECTORY = f"{PROGRAM_DIRECTORY}\\Analysis Sheet Inputs"

def combine_mode_info_files():

    fmt = "{asctime} [{name:40.40}] [{levelname:^10.10}] {message}"
    logging.basicConfig(level=logging.INFO, format=fmt, style="{")

    logger.info("Loading files.")

    short_google_results = pd.read_csv(INPUT_SHORT_FILE_NAME)
    long_google_results = pd.read_csv(INPUT_LONG_FILE_NAME)

    logger.info("Combining and outputting.")

    total_google_results = pd.concat([short_google_results, long_google_results])
    total_google_results.to_csv(f"{OUTPUTS_DIRECTORY}\\mode_info.csv")


if __name__ == "__main__":

    logger = logging.getLogger(__name__)

    fmt = "{asctime} [{name:40.40}] [{levelname:^10.10}] {message}"
    logging.basicConfig(level=logging.INFO, format=fmt, style="{")

    combine_mode_info_files()
