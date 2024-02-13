
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
    ## paths should be relative to where main python scripts are located (probably `src/`)
# ======================

MANUAL_DOWNLOAD_DIR = "data/manual_download"
AUTO_DOWNLOAD_DIR = "data/auto_download"
PROCESSED_DIR = "data/processed"
ACS_PARSED_DATA_DIR = f"{PROCESSED_DIR}/checkpoints"
DATA_DIRS = [MANUAL_DOWNLOAD_DIR, AUTO_DOWNLOAD_DIR, PROCESSED_DIR, ACS_PARSED_DATA_DIR]

ACS_SUMMARY_FILES_DIR = "data/auto_download" # 'sumlevel=xxx' gets added to this

GEO_FILE_STUB = f'Geos{YEAR}{DATASET_YRS}YR'
GEO_FILE_PATH_NO_EXT = f"{AUTO_DOWNLOAD_DIR}/{GEO_FILE_STUB}"
STATE_FIPS_PATH = f"{PROCESSED_DIR}/state_geo_lkup"

LAT_LON_PATH_NO_EXT = f"{PROCESSED_DIR}/lat-lon-area"

CPDB_PATH = f"{AUTO_DOWNLOAD_DIR}/pdb2022bg.csv"
CT_CW_PATH = f"{AUTO_DOWNLOAD_DIR}/2022blockcrosswalk.csv"
SHELL_DF_PATH = f"{AUTO_DOWNLOAD_DIR}/table_shells.csv"

# zip codes
TRACT_ZIP_PATH = f"{MANUAL_DOWNLOAD_DIR}/TRACT_ZIP_092023.xlsx"
TRACT_ZIP_DEDUP_PATH = f"{PROCESSED_DIR}/tract_zip.csv"
BG_ZIP_RAW_PATH = f"{MANUAL_DOWNLOAD_DIR}/geocorr2022.csv"
BG_ZIP_DEDUP_PATH_NO_EXT = f"{PROCESSED_DIR}/bg_zip"

MSA_PATH = f"{MANUAL_DOWNLOAD_DIR}/qcew-county-msa-csa-crosswalk.xlsx"
MSA_SHEET = "Feb. 2013 Crosswalk"

MIL_GEO_IND_PATH_NO_EXT = f"{PROCESSED_DIR}/mil_base_geo_join"

# shapefiles
STATE_SHAPEFILES_OUT_DIR = f"{AUTO_DOWNLOAD_DIR}/tigerweb"
STATE_FOLDER = "state_bg_shapefiles_2020"
US_SHAPEFILE_PATH = f"{PROCESSED_DIR}/us_bg_shapefiles_2020"

# the final data
FINAL_OUTPUT_DIR = "/"
ACS_BG_FILENAME = f"ACS_BG_DATA_{YEAR}"

# ======================
# DEFINED "BY HAND"
# ======================
# Define crosswalk between "new" Connecticut (CT) county entity names and 
# "old" MSA names. (Unable to find or generate a clean, 1:1 crosswalk. And
# there are only 9.)
CT_MSA_CW_DICT = { # ce_name_2022 --> MSA Title
    'Western Connecticut Planning Region, Connecticut': 'Bridgeport-Stamford-Norwalk, CT MSA',
    'Greater Bridgeport Planning Region, Connecticut': 'Bridgeport-Stamford-Norwalk, CT MSA',
    'Naugatuck Valley Planning Region, Connecticut': None, # split between New Haven & Torrington, but probably OK to be left blank?
    'Northwest Hills Planning Region, Connecticut': 'Torrington, CT MicroSA', 
    'Capitol Planning Region, Connecticut': 'Hartford-West Hartford-East Hartford, CT MSA',
    'Lower Connecticut River Valley Planning Region, Connecticut': None, # Middlesex Micro MSA -- techincally part of Hartford MSA but probably OK to be blank
    'South Central Connecticut Planning Region, Connecticut': 'New Haven-Milford, CT MSA',
    'Southeastern Connecticut Planning Region, Connecticut': 'Norwich-New London, CT MSA',
    'Northeastern Connecticut Planning Region, Connecticut': 'Worcester, MA-CT MSA'
}

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