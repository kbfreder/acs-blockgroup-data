


# Design notes

- data processing functions: 
    - some datasets require input -- previously generated summary data, some do not
        - for ease of abstraction, all accept a 'summary_df' input, but not all use it
    - all should return a dataframe with:
        - the "key" column (defined in `configs.py`) from blockgroup summary file tables
        - any newly derived columns
