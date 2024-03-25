
# ACS blockgroup data processing

Process data from the Census 2022 American Community Survey (ACS) at the blockgroup (BG) level.

## TL;DR
1. Download data that can be automated. Frome the `src` folder run: `python prep_all_data.py`
2. Manually download remaining data. See section below.
3. Process the data. From the `src` folder run: `python generate_blockgroup_dataset.py`
4. Final data can be found at:
    a. ACD BG: `./ACS_BG_DATA_2022.csv`
        - there is an accompanying .pkl file that defines the datatypes. See `Processing data` below.
    b. Shapefile: `./data/processed/us_bg_shapefiles_2022`

See Notes below, especially regarding the state of Connecticut (CT; FIPS code 09)


## Downloading data
Various forms of external data must be downloaded prior to processing.


### Automated / scripts
Some datasets have scripts to aid in their download. You can run `src/prep_all_data.py` to trigger all the automated downloading & preparation scripts.
- Run: `cd src`, then `python prep_all_data.py`

Datasets & their scripts:
- `fetch_acs_summary_files.py`
    - Downloads blockgroup-level summary file table data from the census / ACS.

- `fetch_lat_lon_data.py`
    - Downloads latitude+longitude data for blockgroups from TigerWeb. This dataset also include area.
    - Requires that state lookup file exists. See below.

- Optional: `fetch_bg_shapefiles.py`
    - Downloads shapefiles for blockgroups. (Loops over all the states)
    - Should be followed up by running `combine_bg_shapefiles.py`, which then combines the state shapefiles into a single nationwide dataset.
        - requires state lookup file

- `process_zip_data.py`
    - Deduplicates blockgroup-to-zip crosswalk data and extracts blockgroup FIPS.

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
These datasets must be downloaded manually. Save them to `data/manual_download`. (This folder is created by `prep_all_data` script.) Update the respective variables in `config.py` as needed, which hold the filenames.

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


## Processing data

Once all necessary data has been downloaded (see following section), it can be processed using `src/generate_blockgroup_dataset.py`
- Run: `cd src`, then `python generate_blockgroup_dataset.py`
- If only a certain portion of the processing needs to be re-run, you can pass a `--from-step` argument.
- Final Output is saved to `ACS_BG_DATA_2022.csv`
    - A helper dictionary containing the data types for the columns is also created: `ACS_BG_DATA_2022.pkl`. 
      (csv's don't do a great job of retaining datatypes, and some 0-padded strings tend to get interpreted as integers.)
    - The helper function `src/process_bg_tables/util/load_csv_with_dtypes` can be used to load the .csv with the correct datatypes. (ex: `load_csv_with_dtypes('ACS_BG_DATA_2022')`)



## Notes:
- Connecticut (state CT, state code 09):
    - Connecticut changed from counties to "Planning Regions", and with it, the FIPS (INCITS) codes changed: https://www.federalregister.gov/documents/2022/06/06/2022-12063/change-to-county-equivalents-in-the-state-of-connecticut
    - The Census implemented these changes in 2022.
        - The 2022 ACS data has the new FIPS numbers/designations.
        - The 2020 Planning Database does not. 
    - We must therefore leverage a cross-walk to join the two datasets for the state of CT. This is defined by hand -- see `CT_MSA_CW_DICT` in `/src/configs.py`. It's not perfect, but this only impacts data derived from the Planning Database:
        - population density (possibly)
        - median age (possibly)
        - `pct_inst_groupquarters`
        - `pct_non_inst_groupquarters`
        - `non_institutionized_pop` (possibly)
        - `amindian_aknative_hawaiiannative_land_flag`

- Choice of source for certain fields
    - Some data is available from multiple sources, so we must choose. (They are often very similar, but there can be slight differences.) The source used for the first three are defined in `src/configs.py`.

    - Median age: ACS or Census Planning Database

    - Area: TigerWeb Lat/Lon data for each blockgroup or Census Planning Database

    - Non-instituionalized population: ACS or Census Planning Database

    - Military base flag/indicator
        - In the processing for 2019, logic was used to derive this indicator.
            - This logic is retained; See step 4 in `src/generate_blockgroup_dataset.py`
            - The output is stored in the field `military_base_flag`
        - We can also do a spatial join between the lat/lon of military bases and the nationwide shapefile
            - This is performed in `src/download_and_prep_data/geo_merge_mil_bases.py`
            - This output is stored in the field `mil_base_ind`

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




