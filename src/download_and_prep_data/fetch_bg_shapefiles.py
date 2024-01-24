import os, sys
from urllib.request import urlretrieve
from urllib.error import HTTPError
import zipfile
from tqdm import tqdm

sys.path.append("..")
from process_bg_tables.util import load_summary_df
from configs import STATE_TABLE_NAME

# path of this file, relative to parent folder of project/repo
REL_PATH = "../.."
OUTDIR = "data/tigerweb/state_bg_shapefiles_2020/"
BASE_URL = "https://www2.census.gov/geo/tiger/TIGER2022/BG/tl_2022_{}_bg.zip"


def _extract_zip_file(zip_file, destination):
    """Extracts a zip file to a destination directory.

    Args:
    zip_file: The path to the zip file.
    destination: The path to the destination directory.
    """

    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(destination)
    

def fetch_and_extract(state_code, rel_path):
    state_url = BASE_URL.format(state_code)
    state_path = f"{rel_path}/{OUTDIR}{state_code}"
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
    os.makedirs(f"{rel_path}/{OUTDIR}", exist_ok=True)

    print("Fetching blockgroup shapefiles")
    # load state list
    state_lkup_df = load_summary_df(STATE_TABLE_NAME, rel_path=rel_path)
    state_list = state_lkup_df['STATE']

    # iterate over states
    for state_code in tqdm(state_list, desc="states"):
        fetch_and_extract(state_code, rel_path)

    print("")
    
if __name__ == "__main__":
    main(REL_PATH)