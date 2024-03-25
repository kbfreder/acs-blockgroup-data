import os
import sys
from ftplib import FTP

import pandas as pd
from tqdm import tqdm

sys.path.append("..")
from configs import ACS_SUMMARY_FILES_DIR, DATASET_YRS, YEAR

REL_PATH = "../.."


class FetchSummaryLevel:
    def __init__(self, sum_level, year, dataset, rel_path):
        self.sum_level = sum_level
        self.outdir = f"{rel_path}/{ACS_SUMMARY_FILES_DIR}/sumlevel={sum_level}"
        self.ftp_dir =f"/programs-surveys/acs/summary_file/{year}/table-based-SF/data/{dataset}YRData/"
        self.year = year
        self.dataset = dataset
        self.rel_path = rel_path

        os.makedirs(self.outdir, exist_ok=True)


    def _fetch_and_save_file(self, filename):
        file_uri = f"https://www2.census.gov{self.ftp_dir}{filename}"
        try:
            df = pd.read_csv(file_uri, sep="|")
        except Exception as e:
            print(f"Problem accessing: {file_uri} ({e})")
            return None
        
        df = df[ df['GEO_ID'].str.startswith(self.sum_level) ]

        if not df.empty:
            df.to_csv(f"{self.outdir}/{filename}", sep="|", index=False)
            return 0
        else: 
            return None


    def fetch_single_table(self, tbl_id):
        print(f"Downloading table {tbl_id}")
        file = f"acsdt{self.dataset}y{self.year}-{tbl_id.lower()}.dat"
        self._fetch_and_save_file(file)
    

    def fetch_all_tables(self):
        ftp = FTP("ftp2.census.gov")
        ftp.cwd(self.ftp_dir)
        files = [x for x in ftp.nlst() if ".dat" in x]
        num_files = len(files)
        print(f"Will check {num_files} files for sumlevel {self.sum_level}")
        
        c = 0 # counter
        for file in tqdm(files, "Checked: ", unit="files"):
            x = self._fetch_and_save_file(file)
            if x is not None:
                c += 1

        print(f"Found {c} files with sumlevel {self.sum_level} data")


def main(rel_path):
    
    # for summary level codes, see: https://www.census.gov/programs-surveys/acs/geography-acs/geography-boundaries-by-year.html
    ## '150' = blockgroup
    ## '140' = tract
    
    # ------------------
    # blockgroup tables
    # ------------------
    fetch_obj = FetchSummaryLevel(sum_level='150', year=YEAR, dataset=DATASET_YRS, rel_path=rel_path)
    
    ## option 1: download them all (there were 410 in 2022; takes 30-40? min to download them all)
    ## useful if you don't know which tables you need
    # fetch_obj.fetch_all_tables()

    ## option 2: download from a list
    for tbl_id in [
        # Name of table [Universe if not obvious from table name]
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
        fetch_obj.fetch_single_table(tbl_id)


    # ------------------
    # tract tables:
    # ------------------
    
    ## Turns out we don't need any!
    
    # fetch_obj = FetchSummaryLevel(sum_level='140', year=YEAR, dataset=DATASET_YRS, rel_path=rel_path)

    # for tbl_id in [
    #     'B26001', # Group Quarters Population -- don't need, gives same result as a bg-level table
    #     'B08124', # Means of Transportation to Work by Occupation -- Mil Specific Occ's
    #     'B08126', # Means of Transportation to Work by Industry	 -- Armed Forces
    # ]:
    #     fetch_obj.fetch_single_table(tbl_id)



if __name__ == "__main__":
    main(REL_PATH)