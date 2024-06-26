import os
import sys
import zipfile
from urllib.error import HTTPError
from urllib.request import urlretrieve

from tqdm import tqdm

sys.path.append("..")
from configs import STATE_FIPS_PATH, STATE_SHAPEFILES_OUT_DIR, STATE_FOLDER, YEAR
from process_bg_tables.util import load_csv_with_dtypes

# path of this file, relative to parent folder of project/repo
REL_PATH = "../.."
OUT_DIR = os.path.join(STATE_SHAPEFILES_OUT_DIR, STATE_FOLDER)
BASE_URL = "https://www2.census.gov/geo/tiger/TIGER{}/BG/tl_{}_{}_bg.zip"


def _extract_zip_file(zip_file, destination):
    """Extracts a zip file to a destination directory.

    Args:
    zip_file: The path to the zip file.
    destination: The path to the destination directory.
    """

    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(destination)
    

def fetch_and_extract(state_code, rel_path):
    state_url = BASE_URL.format(YEAR, YEAR, state_code)
    state_path = f"{rel_path}/{OUT_DIR}/{state_code}"
    state_zip = f"{state_path}.zip"

    # retrieve file
    try:
        path, msg = urlretrieve(state_url, state_zip)
    except HTTPError as e:
        print(f"Could not retrieve {state_url}: {e}")
        return -1

    # extract file
    _extract_zip_file(state_zip, state_path)

    # delete .zip file
    os.remove(state_zip)


def main(rel_path):
    os.makedirs(f"{rel_path}/{OUT_DIR}", exist_ok=True)

    print("Fetching blockgroup shapefiles")
    # load state list
    state_lkup_df = load_csv_with_dtypes(STATE_FIPS_PATH, rel_path=rel_path)
    state_list = state_lkup_df['STATE']

    # iterate over states
    for state_code in tqdm(state_list, desc="states"):
        fetch_and_extract(state_code, rel_path)

    print("")
    
if __name__ == "__main__":
    main(REL_PATH)