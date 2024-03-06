import os
import sys
from ftplib import FTP

import pandas as pd
from tqdm import tqdm

sys.path.append("..")
from configs import ACS_SUMMARY_FILES_DIR, DATASET_YRS, YEAR

REL_PATH = "../.."


def fetch_table_at_sum_level(tbl_id, year, dataset, sum_level, rel_path):
    """Retrieve table(s) for a given summary level and save locally."""
    # create output directory. 
    outdir = f"{rel_path}/{ACS_SUMMARY_FILES_DIR}/sumlevel={sum_level}"
    os.makedirs(outdir, exist_ok=True)

    ftp_dir =f"/programs-surveys/acs/summary_file/{year}/table-based-SF/data/{dataset}YRData/"

    # get .dat file based on tbl_id or all tables
    if tbl_id == '*':
        ftp = FTP("ftp2.census.gov")
        ftp.cwd(ftp_dir)
        files = [x for x in ftp.nlst() if f"{tbl_id}.dat" in x or (tbl_id=="*" and ".dat" in x)]
        num_files = len(files)
        print(f"Will check {num_files} files for sumlevel {sum_level}")
    else:
        print(f"Attempting to download table {tbl_id}")
        files = [f"acsdt{dataset}y{year}-{tbl_id.lower()}.dat"]

    # TODO: refactor this so we don't print out progress or final count
    # if only downloading a single table
    c = 0 # counter
    for file in tqdm(files, "Checked: ", unit="files"):
        # read data file and query for summary level (http faster than ftp)
        file_uri = f"https://www2.census.gov{ftp_dir}{file}"
        try:
            df = pd.read_csv(file_uri, sep="|")
        except:
            print(f"Could not find {file_uri}")
            return None
        
        df = df[ df['GEO_ID'].str.startswith(sum_level) ]

        if not df.empty:
            c += 1
            df.to_csv(f"{outdir}/{file}", sep="|", index=False)

    print(f"Found {c} files with sumlevel {sum_level} data")


def main(rel_path):
    
    # for summary level codes, see: https://www.census.gov/programs-surveys/acs/geography-acs/geography-boundaries-by-year.html
    ## '150' = blockgroup
    ## '140' = tract
    
    # ------------------
    # blockgroup tables
    # ------------------

    ## option 1: download them all (there were 410 in 2022; takes 30-40? min to download them all)
    ## useful if you don't know which tables you need
    # table_for_sumlevel(tbl_id='*', 
    #                    year=YEAR, 
    #                    dataset=DATASET_YRS, 
    #                    sumlevel='150' # block group
    #                    )

    ## option 2: download from a list
    for tbl_id in [
        # Name of table [Universe (if not obvious from table name)]
        'B01001', # Sex by Age [Total Population]
        'B01002', # Median Age by Sex [Total Population]
        'B01003', # Total Population
        'B02001', # Race [Total population]
        'B03003', # Hispanic or Latino [Total population]
        'B09019', # Household Type (Including Living Alone) by Relationship [Total Population]
        'B11012', # Households by Type [Households]
        'B12001', # Sex by Marital Status for the Population 15 Years and Over
        'B14007', # School Enrollment by Detailed Level of School for the Population 3 Years and Over
        'B15003', # Educational Attainment for the Population 25 Years and Over
        'B19001', # Household Income in the Past 12 Months (in 2022 Inflation-Adjusted Dollars) [Households]
        'B19013', # Median Household Income in the Past 12 Months (in 2022 Inflation-Adjusted Dollars) [Households]
        'B19058', # Public Assistance Income or Food Stamps/SNAP in the Past 12 Months for Households [Households]
        'B23025', # Employment Status for the Population 16 Years and Over
        'B25003', # Tenure [Occupied Housing Units]
        'B25044', # Tenure by Vehicles Available [Occupied Housing Units]
        'B27010', # <any table from:> [Civilian noninstitutionalized population]
        'B28002', # Presence and Types of Internet Subscriptions in Household [Households]
    ]:
        fetch_table_at_sum_level(
            tbl_id=tbl_id,
            year=YEAR, 
            dataset=DATASET_YRS, 
            sum_level='150',
            rel_path=rel_path
        )

    # ------------------
    # tract tables:
    # ------------------
    
    # Turns out we don't need any!
        
    # for tbl_id in [
    #     'B26001', # Group Quarters Population -- don't need, gives same result as a bg-level table
    #     'B08124', # Means of Transportation to Work by Occupation -- Mil Specific Occ's
    #     'B08126', # Means of Transportation to Work by Industry	 -- Armed Forces
    # ]:
    #     table_for_sumlevel(
    #         tbl_id=tbl_id,
    #         year=YEAR, 
    #         dataset=DATASET_YRS, 
    #         sumlevel='140' # tract
    #     )


if __name__ == "__main__":
    main(REL_PATH)