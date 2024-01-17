
from functools import reduce

from util import (
    merge_bg_table_with_summary_df,
    process_some_cols_bg_table,
    # process_single_col_bg_table,
    extract_bg_fips_from_geo_id
    )
from configs import (
    DENOM_TO_USE,
    BG_TABLE_KEY_COL
    )


def get_total_pop():
    tbl_id = 'B01003' # Total Population
    col_idx = 1 # Total
    bg_df = process_some_cols_bg_table(tbl_id, [col_idx], ['population'], 'int')
    return bg_df


def get_non_instit_pop():
    return process_some_cols_bg_table(
        tbl_id='B27010', # random table in 'Civilian noninstitutionalized population' universe
        col_idxs=[1], # Total
        new_col_names=['non_institutionized_pop'],
        dtype='int'
    )

