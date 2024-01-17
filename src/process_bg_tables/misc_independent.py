
from functools import reduce

from util import (
    # merge_bg_table_with_summary_df,
    process_some_cols_bg_table,
    # process_single_col_bg_table
    )
from configs import (
    DENOM_TO_USE,
    BG_TABLE_KEY_COL
    )


def get_median_income():
    """Get median income.

    Does not depend on any previous inputs
    """
    tbl_id = 'B19013' # Median Household Income in the Past 12 Months (in 2022 Inflation-Adjusted Dollars)
    col_idx = 1 # Median household income in the past 12 months...
    bg_df = process_some_cols_bg_table(tbl_id, [col_idx], ['median_income'], 'int')
    return bg_df[[BG_TABLE_KEY_COL, 'median_income']]


def get_grp_qrtrs():
    """Get group quarters population.

    Universe = Total Population
    """
    tbl_id = 'B09019' # Household Type (Including Living Alone) by Relationship 
    col_idxs = [1, 26] # Total, In group quarters
    col_names = ['total',  'grp_quarters_pop']
    bg_df = process_some_cols_bg_table(tbl_id, col_idxs, col_names, 'int')

    return bg_df[[BG_TABLE_KEY_COL, 'grp_quarters_pop']]


def get_military_employed():
    """Get number military employed.

    Used to derive `pct_tract_military_employed` -- denominator calculated later.
    """
    tbl_id = 'B23025' # Employment Status for the Population 16 Years and Over
    col_idxs = [1, 6] # Total, Armed Forces
    col_names = ['total', 'military_employed']

    bg_df = process_some_cols_bg_table(tbl_id, col_idxs, col_names, 'int')

    return bg_df[[BG_TABLE_KEY_COL, 'military_employed']]
