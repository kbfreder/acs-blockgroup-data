
from .util import (
    process_some_cols_bg_table,
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

