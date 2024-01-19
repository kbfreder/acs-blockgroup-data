import numpy as np

from .util import process_some_cols_bg_table
from configs import BG_TABLE_KEY_COL


def _derive_pct_col_name(col):
    return f'race_pct_{col}'

def _derive_pct_pop(df, cols):
    for col in cols:
        df[_derive_pct_col_name(col)] = df[col] / df['total']
    return df


def get_race_pops():
    tbl_id = 'B02001' # Race (Universe = Total population)
    col_idxs = [1, 2, 3, 4, 5]
    race_cols = ['white', 'african_american', 'nativeam', 'asian']
    col_names = ['total'] + race_cols
    bg_df = process_some_cols_bg_table(tbl_id, col_idxs, col_names, 'int')
    pct_bg_df = _derive_pct_pop(bg_df, race_cols)

    pct_race_cols = [_derive_pct_col_name(col) for col in race_cols]

    pct_bg_df['sum_pct_races'] = pct_bg_df[pct_race_cols].sum(axis=1)
    pct_bg_df['race_pct_other'] = 1 - pct_bg_df['sum_pct_races']
    # set small differences to 0
    pct_bg_df['race_pct_other'] = np.where(pct_bg_df['race_pct_other'] < 0, 0, pct_bg_df['race_pct_other'])
    
    pct_race_cols.append('race_pct_other')
    return pct_bg_df[[BG_TABLE_KEY_COL] + pct_race_cols]


def get_hispanic_pop():
    tbl_id = 'B03003'
    col_idxs = [1, 3] # Hispanic or Latino (Universe = Total population)
    col_names = ['total', 'hispanic']
    bg_df = process_some_cols_bg_table(tbl_id, col_idxs, col_names, 'int')
    pct_bg_df = _derive_pct_pop(bg_df, ['hispanic'])
    return pct_bg_df[[BG_TABLE_KEY_COL, _derive_pct_col_name('hispanic')]]

