"""

NOTES: 
- this will take a long time to run!
- you will still need to download some data manually. See README

"""

import os

from process_bg_tables.fips_names import generate_state_fips_data
from download_and_prep_data import (
    combine_bg_shapefiles, 
    fetch_bg_shapefiles,
    fetch_acs_summary_files,
    fetch_lat_lon_data,
    fetch_misc_files,
    process_zip_data,
)
from configs import DATA_DIRS


REL_PATH = ".."

if __name__ == "__main__":
    for data_dir in DATA_DIRS:
        os.makedirs(f"{REL_PATH}/{data_dir}", exist_ok=True)
    
    fetch_misc_files.main(REL_PATH)
    generate_state_fips_data(save_file=True, rel_path=REL_PATH)
    fetch_bg_shapefiles.main(REL_PATH)
    combine_bg_shapefiles.main(REL_PATH)
    fetch_acs_summary_files.main(REL_PATH)
    fetch_lat_lon_data.main(REL_PATH)
    process_zip_data.main(REL_PATH)

    # TODO: add in geo_merge_mil_bases stuff?