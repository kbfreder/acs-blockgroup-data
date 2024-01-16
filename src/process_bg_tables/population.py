
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


def get_total_pop():
    tbl_id = 'B01003' # Total Population
    col_idx = 1 # Total
    bg_df = process_single_col_bg_table(tbl_id, col_idx, 'population', 'int')
    


def get_population_data(*kwargs):
    pass