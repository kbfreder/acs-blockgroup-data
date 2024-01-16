import os, sys
import pandas as pd
# from ftplib import FTP
from urllib.request import urlretrieve
from urllib.error import HTTPError
import zipfile
from tqdm import tqdm


OUTDIR = "../data/tigerweb/state_bg_shapefiles_2020/"
# base url for tigerweb shapefiles for BG's
BASE_URL = "https://www2.census.gov/geo/tiger/TIGER2022/BG/tl_2022_{}_bg.zip"


def extract_zip_file(zip_file, destination):
    """Extracts a zip file to a destination directory.

    Args:
    zip_file: The path to the zip file.
    destination: The path to the destination directory.
    """

    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(destination)
    

# get tables for a summary level and output file for each
def fetch_and_extract(state_code):
    state_url = BASE_URL.format(state_code)
    state_path = f"{OUTDIR}{state_code}"
    state_zip = f"{state_path}.zip"

    # retrieve file
    try:
        path, msg = urlretrieve(state_url, state_zip)
    except HTTPError as e:
        print(f"Could not retrieve {state_url}: {e}")
        return -1

    # extract file
    extract_zip_file(state_zip, state_path)

    # delete .zip file
    os.remove(state_zip)


    

if __name__ == "__main__":
    # load state list
    state_lkup_df = pd.read_csv("../data/state_geo_lkup.csv", 
                                dtype={'GEO_ID': 'object', 'STUSAB': 'object', 'STATE': 'object', 'NAME': 'object'})
    state_list = state_lkup_df['STATE']

    for state_code in tqdm(state_list, desc="states"):
        fetch_and_extract(state_code)
    

