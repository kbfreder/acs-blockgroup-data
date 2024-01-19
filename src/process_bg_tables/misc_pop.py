
from functools import reduce

from .util import (
    process_some_cols_bg_table,
    )
from configs import (
    DENOM_TO_USE,
    BG_TABLE_KEY_COL
    )


def get_pct_college_enrolled(denom, **kwargs):
    assert denom in ['total', 'table'],  f"Unrecognized value ({denom}) provided for denom"
    # if denom == 'total':
    #     assert 'pop_df' in kwargs, "Must supply pop_df when `denom == 'total'`"
    pop_df = kwargs.get('pop_df')

    tbl_id = 'B14007' # 'School Enrollment by Detailed Level of School for the Population 3 Years and Over
    col_idxs = [1, 17, 18] # Total, Enrolled in college undergraduate years
    col_names = ['total', 'enrolled_college', 'grad_or_prof']
    bg_df = process_some_cols_bg_table(tbl_id, col_idxs, col_names, 'int')
    
    if denom == 'total':
        denom_col = 'population'
        bg_df = pop_df.merge(bg_df, on=BG_TABLE_KEY_COL, how='right')
    elif denom == 'table':
       denom_col = 'total'

    bg_df['pct_college_enrolled'] = (
        (bg_df['enrolled_college'] + bg_df['grad_or_prof'])/ bg_df[denom_col])
    
    # return only key col & "col to keep"
    return bg_df[[BG_TABLE_KEY_COL, 'pct_college_enrolled']]


def get_prop_college_educated(denom, **kwargs):

    assert denom in ['total', 'table'],  f"Unrecognized value ({denom}) provided for denom"
    # if denom == 'total':
        # assert 'pop_df' in kwargs, "Must supply pop_df when `denom == 'total'`"
    pop_df = kwargs.get('pop_df')

    tbl_id = 'B15003' # 'Educational Attainment for the Population 25 Years and Over'
    col_idxs = [1, 22, 23, 24, 25] # Total, Bachelor's degree, Masters, Professional, Doctorate
    degree_cols = ['bachelors', 'masters', 'professional', 'doctorate']
    new_col_names = ['total'] + degree_cols
    bg_df = process_some_cols_bg_table(
        tbl_id, col_idxs, new_col_names, 'int')
    
    if denom == 'total':
        denom_col = 'population'
        # bg_df = merge_bg_table_with_pop_df(bg_df, pop_df)
        bg_df = pop_df.merge(bg_df, on=BG_TABLE_KEY_COL, how='right')
    elif denom == 'table':
       denom_col = 'total'
    
    bg_df['total_college_eduated'] = bg_df[degree_cols].sum(axis=1)
    bg_df['prop_college_educated'] = bg_df['total_college_eduated'] / bg_df[denom_col]

    # return only key col & "col to keep"
    return bg_df[[BG_TABLE_KEY_COL, 'prop_college_educated']]



def get_pct_married(denom, **kwargs):
    assert denom in ['total', 'table'],  f"Unrecognized value ({denom}) provided for denom"
    # if denom == 'total':
        # assert 'pop_df' in kwargs, "Must supply pop_df when `denom == 'total'`"
    pop_df = kwargs.get('pop_df')

    tbl_id = 'B12001' # Sex by Marital Status for the Population 15 Years and Over
    col_idxs = [1, 5, 14] # Total, Male: Now married: Married, spouse present;  Female: Now married: Married, spouse present; 
    col_names = ['total', 'male_married', 'female_married']

    bg_df = process_some_cols_bg_table(tbl_id, col_idxs, col_names, 'int')

    if denom == 'total':
        denom_col = 'population'
        # bg_df = merge_bg_table_with_pop_df(bg_df, pop_df)
        bg_df = pop_df.merge(bg_df, on=BG_TABLE_KEY_COL, how='right')
    elif denom == 'table':
       denom_col = 'total'
    
    bg_df['pct_married'] = (bg_df['male_married']  + bg_df['female_married']) / bg_df[denom_col]
    return bg_df[[BG_TABLE_KEY_COL, 'pct_married']]



def get_all_misc_pop_data(**kwargs):
    """
    For some of these calcuations, we have the option of using either the
    total poulation, or the table population as the denominator
    for percent or proportion calculations.

    If using denom == total, must pass `pop_df` kwarg, a dataframe
    containing 'population' field.

    If using denom == table, no other args are needed.

    Which denominator to use is set in configs.py
    """
    # pop_df = kwargs.get('pop_df')
    df_list = []
    for func in [get_pct_college_enrolled,
                 get_prop_college_educated,
                 get_pct_married
                 ]:
        df_list.append(func(DENOM_TO_USE, **kwargs))
    # df_list.append(get_pct_college_enrolled(DENOM_TO_USE, **kwargs))
    # df_list.append(get_prop_college_educated(DENOM_TO_USE, **kwargs))
    # # df_list.append(get_pct_home_ownership(DENOM_TO_USE, **kwargs))
    # df_list.append(get_pct_married(DENOM_TO_USE, **kwargs))

    df = reduce(lambda df1, df2: df1.merge(df2, on=BG_TABLE_KEY_COL), df_list)

    return df
