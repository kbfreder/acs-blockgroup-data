import pandas as pd
from ftplib import FTP
import os, sys
from tqdm import tqdm


OUTDIR = '../data'

# get tables for a summary level and output file for each
def table_for_sumlevel(tblid, year, dataset, sumlevel):

    # create output directory. 
    outdir = f"{OUTDIR}/sumlevel={sumlevel}" #'output'
    # try:
    os.makedirs(outdir, exist_ok=True)
    # except FileExistsError as e:
    #     print(f"directory named '{outdir}' already exists. delete it and try again.")
    #     sys.exit(1)

    # ftp_dir =f"/programs-surveys/acs/summary_file/{year}/prototype/{dataset}YRData/"
    ftp_dir =f"/programs-surveys/acs/summary_file/{year}/table-based-SF/data/{dataset}YRData/"

    print(f"Navigating to: {ftp_dir}")
    #go to ftp site
    ftp = FTP("ftp2.census.gov")
    ftp.login("","")
    ftp.cwd(ftp_dir)

    # get .dat file based on tblid or all tables
    files = [x for x in ftp.nlst() if f"{tblid}.dat" in x or (tblid=="*" and ".dat" in x)]
    num_files = len(files)
    print(f"Will check {num_files} files for sumlevel {sumlevel}")

    c = 0 # counter
    for file in tqdm(files, "Checked: ", unit="files"):
        # read data file and query for summary level (http faster than ftp)
        df = pd.read_csv(f"https://www2.census.gov{ftp_dir}{file}", sep="|")
        df = df[ df['GEO_ID'].str.startswith(sumlevel) ]

        #output
        if not df.empty:
            c += 1
            # print(f"File {file} has data!")
            df.to_csv(f"{outdir}/{file}", sep="|", index=False)
            # print(f"{outdir}/{file} output.")

    print(f"Found {c} files with sumlevel {sumlevel} data")

# #get all tables for all tracts
# table_for_sumlevel(tblid = '*', year=2020, dataset=5, sumlevel='140')


# see: https://www.census.gov/programs-surveys/acs/geography-acs/geography-boundaries-by-year.html
    # for summary level codes
    

if __name__ == "__main__":
    table_for_sumlevel(tblid='*', 
                       year=2022, 
                       dataset=5, 
                       sumlevel='150' # block group
                       )