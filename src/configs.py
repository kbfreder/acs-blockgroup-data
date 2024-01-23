
# ======================
# DEFINED BY ANALYSIS / DATA FORMATS
# ======================

# year of ACS data
YEAR = 2022
DATASET_YRS = 5

# the key or ID column in block group summary tables
BG_TABLE_KEY_COL = 'GEO_ID'


# ======================
# FILENAMES
    ## these could be anything, but need to be consistent across files
# ======================

# this should be relative to where `main.py` is located
BG_DATA_PATH = "data/sumlevel=150/"

STATE_TABLE_NAME = "state_geo_lkup"
GEO_FILE_STUB = f'Geos{YEAR}{DATASET_YRS}YR'
GEO_FILE_PATH_NO_EXT = f"data/parsed_acs_data/{GEO_FILE_STUB}"
LAT_LON_FILENAME = "lat-lon-area"
CPDB_PATH = "data/pdb2022bg.csv"
CT_CW_PATH = "data/2022blockcrosswalk.csv"

# zip codes
TRACT_ZIP_PATH = "data/TRACT_ZIP_092023.xlsx"
TRACT_ZIP_DEDUP_PATH = "data/tract_zip.csv"
BG_ZIP_RAW_PATH = "data/geocorr2022_all-states.csv"
BG_ZIP_DEDUP_PATH = "data/bg_zip.csv"

MSA_PATH = "data/qcew-county-msa-csa-crosswalk.xlsx"
MSA_SHEET = "Feb. 2013 Crosswalk"
ACS_DATA_DIR = "data/parsed_acs_data"
MIL_GEO_IND_PATH = "data/mil_base_geo_join.csv"

# the final data
FINAL_OUTPUT_DIR = "/"
ACS_BG_FILENAME = f"ACS_BG_DATA_{YEAR}"


# ======================
# CHOICES
# ======================

# which denominator to use in pct or prop calculations
## 'total' = total population 
## 'table' = total value provided by table
DENOM_TO_USE = 'total'

# which source to use (ACS vs planning data base (pdb))
## defined in `main.py` instead

# which ZIP code crosswalk to use
ZIP_CHOICES = ['tract', 'blockgroup']
ZIP_SOURCE = ZIP_CHOICES[1]