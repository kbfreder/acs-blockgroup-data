
import pandas as pd

import sys
sys.path.append("..")
from process_bg_tables.util import save_csv_and_dtypes
from configs import YEAR, GEO_FILENAME, GEO_FILE_PATH

def download_geo_documentation():
    print("Retrieving Geo Documentation")
    path = f"https://www2.census.gov/programs-surveys/acs/summary_file/{YEAR}/table-based-SF/documentation/{GEO_FILENAME}"
    geo_df = pd.read_csv(path, sep="|", dtype="str")
    save_csv_and_dtypes(geo_df, GEO_FILE_PATH, rel_path="../..")


if __name__ == "__main__":
    download_geo_documentation()