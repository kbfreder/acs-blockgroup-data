import pandas as pd
from ftplib import FTP
import os, sys
from tqdm import tqdm

from process_bg_tables.configs import YEAR, DATASET_YRS

OUTDIR = '../data'

# get tables for a summary level and output file for each
def table_for_sumlevel(tbl_id, year, dataset, sumlevel):

    # create output directory. 
    outdir = f"{OUTDIR}/sumlevel={sumlevel}" #'output'
    os.makedirs(outdir, exist_ok=True)

    ftp_dir =f"/programs-surveys/acs/summary_file/{year}/table-based-SF/data/{dataset}YRData/"

    # get .dat file based on tbl_id or all tables
    if tbl_id == '*':
        ftp = FTP("ftp2.census.gov")
        # ftp.login("","")
        ftp.cwd(ftp_dir)
        files = [x for x in ftp.nlst() if f"{tbl_id}.dat" in x or (tbl_id=="*" and ".dat" in x)]
        num_files = len(files)
        print(f"Will check {num_files} files for sumlevel {sumlevel}")
    else:
        # acsdt5y2022-b01001g.dat
        print(f"Attempting to download table {tbl_id}")
        files = [f"acsdt{dataset}y{year}-{tbl_id.lower()}.dat"]

    c = 0 # counter
    for file in tqdm(files, "Checked: ", unit="files"):
        # read data file and query for summary level (http faster than ftp)
        file_uri = f"https://www2.census.gov{ftp_dir}{file}"
        try:
            df = pd.read_csv(file_uri, sep="|")
        except:
            print(f"Could not find {file_uri}")
            return None
        
        df = df[ df['GEO_ID'].str.startswith(sumlevel) ]

        #output
        if not df.empty:
            c += 1
            df.to_csv(f"{outdir}/{file}", sep="|", index=False)

    print(f"Found {c} files with sumlevel {sumlevel} data")


# see: https://www.census.gov/programs-surveys/acs/geography-acs/geography-boundaries-by-year.html
# for summary level codes
    

if __name__ == "__main__":
    # blockgroup tables:

    ## option 1: download them all (there were 410 in 2022; takes 30-40? min to download them all)
    ## useful if you don't know which tables you need
    # table_for_sumlevel(tbl_id='*', 
    #                    year=YEAR, 
    #                    dataset=DATASET_YRS, 
    #                    sumlevel='150' # block group
    #                    )

    ## option 2: download from a list
    for tbl_id in [
        'B01001',
        'B01002',
        'B01003',
        'B02001', 
        'B03003', 
        'B09019', 
        'B11012',
        'B12001', 
        'B14007', 
        'B15003', 
        'B19013', 
        'B19058', 
        'B23025', 
        'B25003', 
        'B25044', 
        'B25046', 
        'B27010',
        'B28002', 
    ]:
        table_for_sumlevel(
            tbl_id=tbl_id,
            year=YEAR, 
            dataset=DATASET_YRS, 
            sumlevel='150' # tract
        )

    # tract tables:
    for tbl_id in [
        # 'B26001', # Group Quarters Population -- don't need, gives same result as a bg-level table
        'B08124', # Means of Transportation to Work by Occupation -- Mil Specific Occ's
        'B08126', # Means of Transportation to Work by Industry	 -- Armed Forces
    ]:
        table_for_sumlevel(
            tbl_id=tbl_id,
            year=YEAR, 
            dataset=DATASET_YRS, 
            sumlevel='140' # tract
        )