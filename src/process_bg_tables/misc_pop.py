
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


def get_pct_college_enrolled(denom, **kwargs):
    assert denom in ['total', 'table'],  f"Unrecognized value ({denom}) provided for denom"
    if denom == 'total':
        assert 'summary_df' in kwargs, "Must supply summary_df when `denom == 'total'`"

    tbl_id = 'B14002' # 'Sex by School Enrollment by Level of School by Type of School for the Population 3 Years and Over'
    col_idxs = [1, 19, 43] # Total, Enrolled in college undergraduate years (Male), Enrolled in college undergraduate years (Female)
    col_names = ['total', 'male_enrolled_college', 'female_enrolled_college']
    bg_df = process_multiple_cols_bg_table(tbl_id, col_idxs, col_names, 'int')

    bg_df['total_enrolled_college'] = (bg_df['male_enrolled_college'] + 
                                       bg_df['female_enrolled_college'])
    
    if denom == 'total':
        denom_col = 'population'
        bg_df = merge_bg_table_with_summary_df(bg_df, summary_df)
    elif denom == 'table':
       denom_col = 'total'

    bg_df['pct_college_enrolled'] = bg_df['total_enrolled_college'] / bg_df[denom_col]

    # drop total column, otherwise it may cause trouble
    # return bg_df.drop(columns=['total']), ['pct_college_enrolled']
    
    # return only key col & "col to keep"
    return bg_df[[BG_TABLE_KEY_COL, 'pct_college_enrolled']]


def get_prop_college_educated(denom, **kwargs):

    assert denom in ['total', 'table'],  f"Unrecognized value ({denom}) provided for denom"
    if denom == 'total':
        assert 'summary_df' in kwargs, "Must supply summary_df when `denom == 'total'`"


    tbl_id = 'B15003' # 'Educational Attainment for the Population 25 Years and Over'
    col_idxs = [1, 22] # Total, Bachelor's degree
    new_col_names = ['total', 'bachelors']
    bg_df = process_multiple_cols_bg_table(
        tbl_id, col_idxs, new_col_names, 'int')
    
    if denom == 'total':
        denom_col = 'population'
        bg_df = merge_bg_table_with_summary_df(bg_df, summary_df)
    elif denom == 'table':
       denom_col = 'total'
    
    bg_df['prop_college_educated'] = bg_df['bachelors'] / bg_df[denom_col]

    # return only key col & "col to keep"
    bg_df[[BG_TABLE_KEY_COL, 'prop_college_educated']]


def get_pct_home_ownership(denom, **kwargs):
    assert denom in ['total', 'table'],  f"Unrecognized value ({denom}) provided for denom"
    if denom == 'total':
        assert 'summary_df' in kwargs, "Must supply summary_df when `denom == 'total'`"

    tbl_id = 'B25008' # Total Population in Occupied Housing Units by Tenure
    col_idxs = [1, 2] # Total, Owner occupied
    col_names = ['total', 'owner_occ']
    bg_df = process_multiple_cols_bg_table(tbl_id, col_idxs, col_names, 'int')

    if denom == 'total':
        denom_col = 'population'
        bg_df = merge_bg_table_with_summary_df(bg_df, summary_df)
    elif denom == 'table':
       denom_col = 'total'
    
    bg_df['pct_home_ownership'] = bg_df['owner_occ'] / bg_df[denom_col]

    # return only key col & "col to keep"
    bg_df[[BG_TABLE_KEY_COL, 'pct_home_ownership']]

    
def get_pct_married_grp_qrtrs(summary_df):
    """Get percent married and group quarters population.

    The former calc does not require a previously caculated summary values, 
    as its table's universe is the Total Population

    For the latter, we need the tract population, which is calculated in
    `population.py`.

    Thus, we don't need to specify a denominator, but we do need to pass
    `summary_df`.

    """
    tbl_id = 'B09019' # Household Type (Including Living Alone) by Relationship
    col_idxs = [1, 10, 11, 26] # Total, Opposite-sex spouse, Same-sex spouse, In group quarters
    col_names = ['total', 'opp_sex_spouse', 'same_sex_spouse', 'total_grp_quarters']
    bg_df = process_multiple_cols_bg_table(tbl_id, col_idxs, col_names, 'int')

    bg_df = merge_bg_table_with_summary_df(bg_df, summary_df)

    # PCT_MARRIED
    bg_df['pct_married'] = (bg_df['opp_sex_spouse'] + bg_df['same_sex_spouse']) / bg_df['population']

    # PCT_TRACT_GROUPQTRS
    bg_df['tract_grp_quarters'] = bg_df.groupby("census_tract_fips")['total_grp_quarters'].transform("sum")
    bg_df['pct_tract_groupqtrs'] = bg_df['tract_grp_quarters'] / bg_df['tract_pop']

    return bg_df[[BG_TABLE_KEY_COL, 'pct_married', 'pct_tract_groupqtrs']]



def get_pct_married(denom, **kwargs):
    assert denom in ['total', 'table'],  f"Unrecognized value ({denom}) provided for denom"
    if denom == 'total':
        assert 'summary_df' in kwargs, "Must supply summary_df when `denom == 'total'`"

    tbl_id = 'B12001' # Sex by Marital Status for the Population 15 Years and Over
    col_idxs = [1, 5, 14] # Total, Male: Now married: Married, spouse present;  Female: Now married: Married, spouse present; 
    col_names = ['total', 'male_married', 'female_married']

    bg_df = process_multiple_cols_bg_table(tbl_id, col_idxs, col_names, 'int')

    if denom == 'total':
        denom_col = 'population'
        bg_df = merge_bg_table_with_summary_df(bg_df, summary_df)
    elif denom == 'table':
       denom_col = 'total'
    
    bg_df['pct_married'] = (bg_df['male_married']  + bg_df['female_married']) / bg_df[denom_col]
    return bg_df[[BG_TABLE_KEY_COL, 'pct_married']]


def get_military_employed(summary_df):
    """
    For this calculation, we need the tract population, which is calculated in
    `population.py`.

    Thus, we don't need to specify a denominator, but we do need to pass
    `summary_df`.
    """

    tbl_id = 'B23025' # Employment Status for the Population 16 Years and Over
    col_idxs = [1, 6] # Total, Armed Forces
    col_names = ['total', 'military_employed']

    bg_df = process_multiple_cols_bg_table(tbl_id, col_idxs, col_names, 'int')
    bg_df = merge_bg_table_with_summary_df(bg_df, summary_df)

    bg_df['tract_military_employed'] = bg_df.groupby('census_tract_fips')['military_employed'].transform('sum')
    bg_df['pct_tract_military_employed'] = bg_df['tract_military_employed'] / bg_df['tract_pop']

    return bg_df[[BG_TABLE_KEY_COL, 'pct_tract_military_employed']]


def get_all_misc_data(**kwargs):
    """
    For some of these calcuations, we have the option of using either the
    total poulation, or the table population as the denominator
    for percent or proportion calculations.

    If using denom == total, must pass `summary_df` kwarg, a dataframe
    containing 'population' field.

    If using denom == table, no other args are needed.

    DENOM_TO_USE is set in configs.py
    """

    df_list = []
    df_list.append(get_pct_college_enrolled(DENOM_TO_USE, **kwargs))
    df_list.append(get_prop_college_educated(DENOM_TO_USE, **kwargs))
    df_list.append(get_pct_home_ownership(DENOM_TO_USE, **kwargs))
    df_list.append(get_pct_married(DENOM_TO_USE, **kwargs))
    df_list.append(get_military_employed(**kwargs))

    df = reduce(lambda df1, df2: df1.merge(df2, on=BG_TABLE_KEY_COL), df_list)

    return df
