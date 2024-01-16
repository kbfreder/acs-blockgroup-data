

import os
import pandas as pd
import re


shell_ftp_dir = "https://www2.census.gov/programs-surveys/acs/summary_file/2022/table-based-SF/documentation/ACS20225YR_Table_Shells.txt"
shell_df = pd.read_csv(shell_ftp_dir, sep="|", )

# just pull out table names
table_list_df = shell_df[['Table ID', 'Title', 'Universe', 'Type']].drop_duplicates()
table_list = table_list_df['Title'].to_list()

# filter on those that contain block group data (which we downloaded from the census website via FTP, using a script)
file_list = os.listdir("../data/sumlevel=150/")
bg_table_id_list = [re.search(r"[bc]\d{4,5}\w?", f).group().upper() for f in file_list]
bg_table_list_df = table_list_df[table_list_df['Table ID'].isin(bg_table_id_list)]
bg_table_list = bg_table_list_df['Title'].to_list()

# just pull out universe names
universe_list_df = bg_table_list_df.groupby(['Universe', 'Type']).agg({
    'Title': list,
    'Table ID': list
})
universe_list_df.reset_index(inplace=True, drop=False)
universe_list = universe_list_df['Universe'].to_list()
