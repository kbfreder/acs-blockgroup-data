
# ACS blockgroup data processing

Process (extract & derive) data from the 2022 American Community Survey (ACS) from the Census, at the blockgroup level.


## Downloading data

- `src/fetch_acs_summary_files.py`
    - Downloads all blockgroup-level summary file table data from the census / ACS

- `src/fetch_lat_lon_data.py`
    - Downloads latitude+longitude data for blockgroups from TigerWeb. This dataset also include area.

- Optional: `src/fetch_bg_shapefiles.py`
    - Download shapefiles for blockgroups. (Loops over all the states, then combines into a single dataset.)



## Notes:
- Connecticut (state code CT or 09):
    - Connecticut changed from counties to "Planning Regions", and with it, the FIPS (INCITS) codes changed: https://www.federalregister.gov/documents/2022/06/06/2022-12063/change-to-county-equivalents-in-the-state-of-connecticut
        - "To assist with the transition from counties to planning regions and the development of longitudinal data for the new county-equivalents, the Census Bureau will produce and make available reference files identifying the cities and towns that constitute each planning region, and reference files identifying the relationships between various sub-state and sub-county geographic areas and the planning regions. This will facilitate aggregation of data from Census Bureau programs that collect, tabulate, and disseminate data for cities and towns in Connecticut. These files will be posted at the Census Bureau website titled “Substantial Changes to Counties and County-Equivalent Entities: 1970–Present” and will include detailed information about the updates referenced in this notice."
    - Thus, it's tricky to compare pre-2022 CT data to 2022 & after data.
    - pre-2022 includes Census Data, and the Census Planning Database, which we get some data from.