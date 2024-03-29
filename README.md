
# ACS blockgroup data processing

Process data from the Census 2022 American Community Survey (ACS) at the blockgroup level.

## Processing data

Once all necessary data has been downloaded (see following section), it can be processed using `src/generate_blockgroup_dataset.py`
- Run: `cd src`, then `python generate_blockgroup_dataset.py`
- If only a certain portion of the processing needs to be re-run, you can pass a `--from-step` argument.
- Final Output is saved to `ACS_BG_DATA_2022.csv`
    - A helper dictionary containing the data types for the columns is also created: `ACS_BG_DATA_2022.pkl`. 
      (csv's don't do a great job of retaining datatypes, and some 0-padded strings tend to get interpreted as integers.)
    - The helper function `src/process_bg_tables/util/load_csv_with_dtypes` can be used to load the .csv with the correct datatypes. (ex: `load_csv_with_dtypes('ACS_BG_DATA_2022')`)


## Downloading data
External data must be downloaded prior to processing.


### Automated / scripts
Some datasets have scripts to aid in their download. You can run `src/prep_all_data.py` to trigger all the automated downloading & preparation scripts.
- Run: `cd src`, then `python prep_all_data.py`

Datasets & their scripts:
- `fetch_acs_summary_files.py`
    - Downloads blockgroup-level summary file table data from the census / ACS.

- `fetch_lat_lon_data.py`
    - Downloads latitude+longitude data for blockgroups from TigerWeb. This dataset also include area.
    - Requires that state lookup file exists. See below

- Optional: `fetch_bg_shapefiles.py`
    - Download shapefiles for blockgroups. (Loops over all the states, then combines into a single dataset.)
    - Should be followed up by running `combine_bg_shapefiles.py`
        - requires state lookup file

- `process_zip_data.py`
    - Deduplicates blockgroup-to-zip crosswalk data and extract blockgroup FIPS.

- `fetch_misc_files.py`
    - Blockgroup "Geo" Documentation
    - Census Planning Database: 
        - Download from: https://www2.census.gov/adrm/PDB/2022/pdb2022bg.csv
        - Config variable: `CPDB_PATH`
    - CT county to planning region crosswalk:
        - See note below.
        - Download from: https://github.com/CT-Data-Collaborative/2022-tract-crosswalk
        - Config variable: `CT_CW_PATH`
 

### Manual:
These datasets must be downloaded manually. Save them to `data/manual_download`. (Folder should be created by `prep_all_data` script.) Update the respective variables in `config.py` as needed, which hold the filenames.

- County-MSA crosswalk:
    - Download from: https://www.bls.gov/cew/classifications/areas/qcew-county-msa-csa-crosswalk.xlsx
    - Filename: `qcew-county-msa-csa-crosswalk.xlsx`
    - Config variables: `MSA_PATH` and `MSA_SHEET`

- Zip crosswalk:
    - Option: Tract-Zip crowsswalk (from HUD):
        - Donwload from: https://www.huduser.gov/portal/datasets/usps_crosswalk.html
            - Requires registration
            - Select 'TRACT-ZIP' crosswalk type
        - Config variable: `TRACT_ZIP_PATH`
    - Option: Blockgroup-Zip crosswalk:
        - Downlaod from: https://mcdc.missouri.edu/applications/geocorr2022.html
            - Select all states
            - Source geo = Census block group
            - Target geo = ZIP/ZCTA
            - Weighting variable = Population (default)
        - Filename: `geocorr2022_xxxxxxxxx.csv`
        - Rename the downloaded file: remove the `_xxx` after `geocorr2022` and before `.csv`
        - Config variables: `BG_ZIP_RAW_PATH` 
    - Set `ZIP_SOURCE` in `configs.py` to indicate which option to use




## Notes:
- Connecticut (state CT, state code 09):
    - Connecticut changed from counties to "Planning Regions", and with it, the FIPS (INCITS) codes changed: https://www.federalregister.gov/documents/2022/06/06/2022-12063/change-to-county-equivalents-in-the-state-of-connecticut
    - The Census implemented these changes in 2022.
        - The 2022 ACS data has the new FIPS numbers/designations, 
        - The 2020 Planning Database does not. 
    - We must therefore leverage a cross-walk to join the two datasets for the state of CT


- Jam Values
    - Per: https://www.census.gov/content/dam/Census/library/publications/2023/acs/acs_table_based_summary_file_handbook.pdf:
        A “jam value” is a hard-coded value used to explain
        the absence of data. The Table-Based Summary
        File uses numeric jam values, whereas, the previous
        sequence-based format used character values. For
        example, a jam value is represented by a value in the
        data display, such as “-666666666,” in cases where
        the estimate could not be computed because there
        was an insufficient number of sample observations.
        The sequence-based format used a dot (.) to
        express this information. Learn more about these
        special data values on the Census Bureau’s “Code
        Lists, Definitions, and Accuracy" webpage.
        22

    - e.g. "-666666666" represents: "Estimate not computed due to insufficient number of sample cases."

    - Link to jam value lookup: https://www2.census.gov/programs-surveys/acs/tech_docs/jam_values/2022_Jam_Values.xlsx

    - Jam Values observed in 
        - median income (table B19013)
        - median age?


- Households vs occupied housing units:
    - Per census definitions PDF (see below): "The count of occupied housing units is the same as the count of households."


## References
- Relationship between 2010 & 2020 block groups: https://www2.census.gov/geo/docs/maps-data/data/rel2020/blkgrp/tab20_blkgrp20_blkgrp10_natl.txt
- Definitions: https://www.census.gov/housing/hvs/definitions.pdf




