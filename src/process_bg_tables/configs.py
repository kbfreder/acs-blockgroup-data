
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
# ======================
# thes could be anything

# BASE_TABLE_NAME = "bg_fips"

# this should be relative to where `main.py` is located
BG_DATA_PATH = "../data/sumlevel=150/"
STATE_TABLE_NAME = "state_fips"
LAT_LON_FILENAME = "lat-lon-area"
ACS_BG_FILENAME = f"ACS_BG_DATA_{YEAR}"


# ======================
# CHOICES
# ======================

# which denominator to use in pct or prop calculations
## 'total' = total population 
## 'table' = total value provided by table
DENOM_TO_USE = 'total'

# which area column/source to use
sources = ['acs', 'planning_data_base']
AREA_SOURCE = sources[1]
median_age_source = sources[0]
non_inst_pop_source = sources[0]