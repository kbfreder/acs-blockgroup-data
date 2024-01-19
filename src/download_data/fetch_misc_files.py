
import pandas as pd

import sys
sys.path.append("..")
from process_bg_tables.util import save_csv_and_dtypes
from configs import YEAR, GEO_FILE_STUB, GEO_FILE_PATH_NO_EXT

def download_geo_documentation():
    print("Retrieving Geo Documentation")
    path = f"https://www2.census.gov/programs-surveys/acs/summary_file/{YEAR}/table-based-SF/documentation/{GEO_FILE_STUB}.txt"
    geo_df = pd.read_csv(path, sep="|", dtype="str")
    print("Saving")
    save_csv_and_dtypes(geo_df, GEO_FILE_PATH_NO_EXT, rel_path="../..")


if __name__ == "__main__":
    download_geo_documentation()