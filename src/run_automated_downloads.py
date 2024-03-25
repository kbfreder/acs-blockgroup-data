"""

NOTES: 
- this will take a long time to run!
- you will still need to download some data manually. See README

"""

import os
import argparse

from process_bg_tables.fips_names import generate_state_fips_data
from download_and_prep_data import (
    combine_bg_shapefiles,
    fetch_bg_shapefiles,
    fetch_acs_summary_files,
    fetch_lat_lon_data,
    fetch_misc_files,
    geo_merge_mil_bases
)
from configs import (DATA_DIRS)


REL_PATH = ".."

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--skip-shapefiles',
        help="Skip the downloading and combining of state shapefiles",
        action="store_true",
        default=False,
        required=False
    )
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parse_args()
    skip_shapefiles = args.skip_shapefiles

    for data_dir in DATA_DIRS:
        os.makedirs(f"{REL_PATH}/{data_dir}", exist_ok=True)
    
    fetch_misc_files.main(REL_PATH)
    generate_state_fips_data(save_file=True, rel_path=REL_PATH)
    fetch_acs_summary_files.main(REL_PATH)
    fetch_lat_lon_data.main(REL_PATH)
    
    if not skip_shapefiles:
        fetch_bg_shapefiles.main(REL_PATH)
        combine_bg_shapefiles.main(REL_PATH)
        # this depends on the presence of shapefiles
        geo_merge_mil_bases.main(REL_PATH)
    else:
        print("Skipping shapefiles")
    
    print("Done with automated downloads! Please manually download remaining data.")