
import os, sys

from process_bg_tables.fips_names import generate_bg_fips_data
from process_bg_tables.population import get_total_pop, get_non_instit_pop
from process_bg_tables.households import get_all_household_data
from process_bg_tables.age import generate_age_prop_data, get_median_age
from process_bg_tables.race import get_race_pops, get_hispanic_pop
from process_bg_tables.misc_independent import (
    get_median_income, get_grp_qrtrs, get_military_employed
)
from process_bg_tables.misc_pop import get_all_misc_pop_data

from process_bg_tables.util import (
    extract_bg_fips_from_geo_id,
    load_summary_df,
    save_summary_df
)
from configs import (
    LAT_LON_FILENAME,
    ACS_BG_FILENAME,
    BG_TABLE_KEY_COL,
    TRACT_ZIP_PATH,
    MSA_PATH, MSA_SHEET
)


print("All imports successful")
print(ACS_BG_FILENAME)