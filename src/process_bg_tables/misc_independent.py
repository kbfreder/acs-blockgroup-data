
from functools import reduce

from util import (
    merge_bg_table_with_summary_df,
    process_multiple_cols_bg_table,
    process_single_col_bg_table
    )
from configs import (
    DENOM_TO_USE,
    BG_TABLE_KEY_COL
    )


def get_median_income():
    """
    Does not depend on any previous inputs
    """
    tbl_id = 'B19013' # Median Household Income in the Past 12 Months (in 2022 Inflation-Adjusted Dollars)
    col_idx = 1 # Median household income in the past 12 months...
    bg_df = process_single_col_bg_table(tbl_id, col_idx, 'median_income', 'int')
    return bg_df[[BG_TABLE_KEY_COL, 'median_income']]


def get_misc_independent_data():
    df_list = []
    df_list.append(get_median_income())

    # TODO: finish this
