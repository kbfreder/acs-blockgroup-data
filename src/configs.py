
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
GEO_FILENAME = f'Geos{YEAR}{DATASET_YRS}YR.txt'
GEO_FILE_PATH = f"data/{GEO_FILENAME}"
LAT_LON_FILENAME = "lat-lon-area"
CPDB_PATH = "data/pdb2022bg.csv"
CT_CW_PATH = "data/2022blockcrosswalk.csv"
TRACT_ZIP_PATH = "data/TRACT_ZIP_092023.xlsx"
MSA_PATH = "data/qcew-county-msa-csa-crosswalk.xlsx"
MSA_SHEET = "Feb. 2013 Crosswalk"

ACS_DATA_DIR = "data/parsed_acs_data"
ACS_BG_FILENAME = f"ACS_BG_DATA_{YEAR}"


# ======================
# CHOICES
# ======================

# which denominator to use in pct or prop calculations
## 'total' = total population 
## 'table' = total value provided by table
DENOM_TO_USE = 'total'

# defined in `main.py` instead
# # which area column/source to use
# sources = ['acs', 'planning_data_base']
# AREA_SOURCE = sources[1]
# median_age_source = sources[0]
# non_inst_pop_source = sources[0]