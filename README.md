
# ACS blockgroup data processing

Instructions for obtaining and processing data from the Census 2022 American Community Survey (ACS) at the blockgroup (BG) level.

## OVERVIEW
1. Download data that can be automated. Frome the `src` folder run: `python prep_all_data.py`
2. Manually download remaining data. See section below.
3. Process the data. From the `src` folder run: `python generate_blockgroup_dataset.py`
4. Final data can be found at:
    a. ACD BG: `./ACS_BG_DATA_2022.csv`
        - there is an accompanying .pkl file that defines the datatypes. See `Processing data` below.
    b. Shapefiles: `./data/processed/us_bg_shapefiles_2022`

See Notes below, especially regarding the state of Connecticut (CT; FIPS code 09).


## Data Sources
Various forms of external data must be downloaded before the blockgroup dataset can be generated.

### Automated downloads

Some datasets have scripts to aid in their download. You can run `src/prep_all_data.py` to trigger all the automated downloading & data preparation scripts.
- Navigate to the `src` folder, then run `python prep_all_data.py`
    - Pass the argument `--skip-shapefiles` to skip the downloading and combining of shapefiles (saves 5-10 minutes; they are not needed for generation of the blockgroup dataset)
- The scripts referenced below are located in `src/download_and_prep_data/`.


|Dataset | Script | Dependency | Notes |
|--------|--------|-------|------------|
|ACS Summary Tables | `fetch_acs_summary_files.py` | None | This is the main source of data |
|BG Lat/Lon         | `fetch_lat_lon_data.py` | state list | includes area |
|Shapefiles         |  `fetch_bg_shapefiles.py` + `combine_bg_shapefiles.py` | state list | First script loops over states & downloads state shapefiles. Second script combines them into nationwide shapefiles |
|BG Geo Documentation | `fetch_misc_files.py` | None | contains BG FIPS, names |
|Census Planning Database | `fetch_misc_files.py` | None | |
|Connecticut Crosswalk | `fetch_misc_files.py` | None | See Note below| 
|State List | n/a | BG Geo Documentation | dervied from Geo Documentation; generated as part of `prep_all_data.py`|
|Military Bases Geo Location | `geo_merge_mil_bases.py` | US Shapefiles | |


### Manual downloads

Other datasets must be manually downloaded. Save them to `data/manual_download`. (This folder is created by `prep_all_data` script.) Update the respective variables in `config.py` as needed, which hold the filenames.

|Dataset | URL | Config variable(s) | Notes |
|--------|-----|--------------------|------------|
|County-MSA crosswalk | https://www.bls.gov/cew/classifications/areas/qcew-county-msa-csa-crosswalk.xlsx | `MSA_PATH` and `MSA_SHEET` | |
|Zip Crosswalk - Tract | https://www.huduser.gov/portal/datasets/usps_crosswalk.html | `TRACT_ZIP_PATH` | site requires registrations; select 'TRACT-ZIP' crosswalk type when downloading |
|Zip Crosswalk - BG | https://mcdc.missouri.edu/applications/geocorr2022.html | `BG_ZIP_RAW_PATH` | See below for download instructions |


- Blockgroup-Zip crosswalk download instructions
    - Options to select:
        - Select all states
        - Source geo = Census block group
        - Target geo = ZIP/ZCTA
        - Weighting variable = Population (default)
    - Filename will be: `geocorr2022_YYYYMMDDHHMM.csv` (where the `_YYYYMMDDHHMM` corresponds to the datetime of the download)
        - Rename the downloaded file: remove the `_YYYYMMDDHHMM` after `geocorr2022` and before `.csv`



## Processing data to derive the blockgroup dataset

Once all necessary data has been downloaded, it can be processed using `src/generate_blockgroup_dataset.py`
- Navigate to the `src` folder, then run `python generate_blockgroup_dataset.py`
    - If only a certain portion of the processing needs to be re-run, you can pass a `--from-step` argument.
- Final Output is saved to `ACS_BG_DATA_2022.csv`
    - A helper dictionary containing the data types for the columns is also created: `ACS_BG_DATA_2022.pkl`. 
      (csv's don't do a great job of retaining datatypes, and some 0-padded strings tend to get interpreted as integers.)
    - The helper function `src/process_bg_tables/util/load_csv_with_dtypes` can be used to load the .csv with the correct datatypes. (ex: `load_csv_with_dtypes('ACS_BG_DATA_2022')`)



## Notes:
### Connecticut (state CT, state code 09):
Connecticut changed from counties to "Planning Regions", and with it, the FIPS (INCITS) codes changed: https://www.federalregister.gov/documents/2022/06/06/2022-12063/change-to-county-equivalents-in-the-state-of-connecticut

The Census implemented these changes in 2022.
- The 2022 ACS data has the new FIPS numbers/designations.
- The 2020 Planning Database does not. 

We must therefore leverage a cross-walk to join the two datasets for the state of CT. 
- A comprehensive one exists at the Block level (see source URL in `src/download_and_prep_data/fetch_misc_files.py`). We can then rollup to the blockgroup level; this is done in step 3 of `src/generate_blockgroup_dataset.py`.
- An exact mapping for MSA nams could not be found. We use a manually-defined lookup instead; it is defined in `src/configs.py`

### Choice of source for certain fields
Some data is available from multiple sources, so we must choose. (They are often very similar, but there can be slight differences.) 

The source used for the following are defined in `src/configs.py`:

- Median age: ACS or Census Planning Database

- Area: TigerWeb Lat/Lon data for each blockgroup or Census Planning Database

- Non-instituionalized population: ACS or Census Planning Database

- Zip Crosswalk: Tract-level or BG-level


The data from both sources are retained for the following:

- Military base flag/indicator
    - In the processing for 2019, logic was used to derive this indicator.
        - This logic is retained; See step 4 in `src/generate_blockgroup_dataset.py`
        - The output is stored in the field `military_base_flag`
    - We can also do a spatial join between the lat/lon of military bases and the nationwide shapefile
        - This is performed in `src/download_and_prep_data/geo_merge_mil_bases.py`
        - This output is stored in the field `military_base_flag_geo`

### Jam Values
Per: https://www.census.gov/content/dam/Census/library/publications/2023/acs/acs_table_based_summary_file_handbook.pdf:

    A “jam value” is a hard-coded value used to explain
    the absence of data...For example, a jam value is 
    represented by a value in the data display, such as 
    “-666666666,” in cases where the estimate could not 
    be computed because there was an insufficient number 
    of sample observations.

- Link to jam value lookup: https://www2.census.gov/programs-surveys/acs/tech_docs/jam_values/2022_Jam_Values.xlsx

- Jam Values are observed in 
    - median income (table B19013)
    - median age


### Households vs occupied housing units:
- Per census definitions PDF (see below): "The count of occupied housing units is the same as the count of households."


## References
- ACS: https://www.census.gov/programs-surveys/acs.html
- Census Planning Database: https://www.census.gov/topics/research/guidance/planning-databases.html
- Relationship between 2010 & 2020 block groups: https://www2.census.gov/geo/docs/maps-data/data/rel2020/blkgrp/tab20_blkgrp20_blkgrp10_natl.txt
- Definitions: https://www.census.gov/housing/hvs/definitions.pdf

