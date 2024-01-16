library (RODBC)
library (tidyverse)
library (lubridate)


###### Blockgroup Census Data #####
# csvs from 2019 ACS Survey (via ACS_2019_downloading.ipynb notebook)
# Moved csvs from /domino/datasets/u0050321/.../scratch/ to 
# /mnt/blockgroup_data_importing/ACS_2019_5year/
################

test <- read_csv("/mnt/blockgroup_data_importing/ACS_2019_5year/TOTAL_POPULATION_IN_OCCUPIED_HOUSING_UNITS_BY_TENURE.csv")
zz <- read_csv("/domino/datasets/u0046259/Site_Selection_Blockgroup2021/scratch/aaai_cache/TENURE.csv")

# Couldnt get military base info to pull.using occupatoin from this csv to get military
fips_military <- read_csv('/domino/datasets/u0046259/Site_Selection_Blockgroup2021/scratch/aaai_cache/MEANS_OF_TRANSPORTATION_TO_WORK_BY_OCCUPATION.csv')
fips_college_enrolled <- read_csv('/domino/datasets/u0046259/Site_Selection_Blockgroup2021/scratch/aaai_cache/SCHOOL_ENROLLMENT_BY_LEVEL_OF_SCHOOL_FOR_THE_POPULATION_3_YEARS_AND_OVER.csv')
fips_hispanic_race <- read_csv ('/domino/datasets/u0046259/Site_Selection_Blockgroup2021/scratch/aaai_cache/HISPANIC_OR_LATINO_ORIGIN_BY_RACE.csv')
fips_age <- read_csv ('/domino/datasets/u0046259/Site_Selection_Blockgroup2021/scratch/aaai_cache/SEX_BY_AGE.csv')
fips_housing <- read_csv('/domino/datasets/u0046259/Site_Selection_Blockgroup2021/scratch/aaai_cache/TENURE.csv')
fips_marital <- read_csv('/domino/datasets/u0046259/Site_Selection_Blockgroup2021/scratch/aaai_cache/SEX_BY_MARITAL_STATUS_FOR_THE_POPULATION_15_YEARS_AND_OVER.csv')
fips_internet <- read_csv('/domino/datasets/u0046259/Site_Selection_Blockgroup2021/scratch/aaai_cache/PRESENCE_AND_TYPES_OF_INTERNET_SUBSCRIPTIONS_IN_HOUSEHOLD.csv')
fips_vehicles <- read_csv('/domino/datasets/u0046259/Site_Selection_Blockgroup2021/scratch/aaai_cache/NUMBER_OF_WORKERS_IN_HOUSEHOLD_BY_VEHICLES_AVAILABLE.csv')
fips_population <- read_csv ('/domino/datasets/u0046259/Site_Selection_Blockgroup2021/scratch/aaai_cache/TOTAL_POPULATION.csv')
fips_education <- read_csv('/domino/datasets/u0046259/Site_Selection_Blockgroup2021/scratch/aaai_cache/EDUCATIONAL_ATTAINMENT_FOR_THE_POPULATION_25_YEARS_AND_OVER.csv')
fips_race <- read_csv ('/domino/datasets/u0046259/Site_Selection_Blockgroup2021/scratch/aaai_cache/RACE.csv')
fips_income <- read_csv ('/domino/datasets/u0046259/Site_Selection_Blockgroup2021/scratch/aaai_cache/MEDIAN_HOUSEHOLD_INCOME_IN_THE_PAST_12_MONTHS_IN_2019_INFLATION_ADJUSTED_DOLLARS.csv')
fips_foodstamps <- read_csv('/domino/datasets/u0046259/Site_Selection_Blockgroup2021/scratch/aaai_cache/RECEIPT_OF_FOOD_STAMPS_SNAP_IN_THE_PAST_12_MONTHS_BY_DISABILITY_STATUS_FOR_HOUSEHOLDS.csv')
fips_qtrs <- read_csv('/domino/datasets/u0046259/Site_Selection_Blockgroup2021/scratch/aaai_cache/GROUP_QUARTERS_POPULATION.csv')
fips_income_bands <- read_csv('/domino/datasets/u0046259/Site_Selection_Blockgroup2021/scratch/aaai_cache/HOUSEHOLD_INCOME_IN_THE_PAST_12_MONTHS_IN_2019_INFLATION_ADJUSTED_DOLLARS.csv')

fips_income2 <- dplyr::mutate (fips_income, median_income = `Median_household_income_in_the_past_12_months_in_2019_inflation_adjusted_dollars`) %>%
  dplyr::select (fips, median_income)

fips_pop2 <- dplyr::mutate (fips_population, population=Total) %>% dplyr::select(state, fips, population)

fips_foodstamps2 <- dplyr::mutate (fips_foodstamps,
                            pct_hh_foodstamps = Total_Household_received_Food_Stamps_SNAP_in_the_past_12_months/Total) %>%
                    dplyr::rename (households = Total) %>%
                    dplyr::select (fips, households, pct_hh_foodstamps)

fips_age2 <- dplyr::mutate (fips_age, 
                     prop_age_under_18 =  (Total_Male_Under_5_years+Total_Female_Under_5_years+
                                       Total_Male_5_to_9_years+Total_Female_5_to_9_years+
                                       Total_Male_10_to_14_years+Total_Female_10_to_14_years+
                                       Total_Male_15_to_17_years+Total_Female_15_to_17_years)/Total,
                       
                     prop_age_18_29 =  (`Total_Female_18_and_19_years`+`Total_Female_20_years`+
                                          `Total_Female_21_years`+`Total_Female_22_to_24_years`+`Total_Female_25_to_29_years` +
                                          `Total_Male_18_and_19_years`+`Total_Male_20_years`+
                                          `Total_Male_21_years`+`Total_Male_22_to_24_years`+`Total_Male_25_to_29_years`)/Total,
                     
                     prop_age_50_64 =  (`Total_Female_50_to_54_years` + `Total_Female_55_to_59_years` +
                                          `Total_Female_60_and_61_years` + `Total_Female_62_to_64_years` +
                                          `Total_Male_50_to_54_years` + `Total_Male_55_to_59_years` +
                                          `Total_Male_60_and_61_years` + `Total_Male_62_to_64_years`)/Total,
                     
                     prop_age_65_69 =  (`Total_Female_65_and_66_years` + `Total_Female_67_to_69_years` +
                                          `Total_Male_65_and_66_years` + `Total_Male_67_to_69_years`)/Total,
                     
                     # These are for dashboard
                     
                     prop_age_18_24 = (`Total_Female_18_and_19_years`+`Total_Female_20_years`+
                                         `Total_Female_21_years`+`Total_Female_22_to_24_years` +
                                         `Total_Male_18_and_19_years`+`Total_Male_20_years`+
                                         `Total_Male_21_years`+`Total_Male_22_to_24_years`)/Total,
                     
                     prop_age_25_34 = (`Total_Female_25_to_29_years`+`Total_Female_30_to_34_years`+
                                         `Total_Male_25_to_29_years`+`Total_Male_30_to_34_years`)/Total,
                     
                     prop_age_35_44 = (`Total_Female_35_to_39_years`+`Total_Female_40_to_44_years`+
                                         `Total_Male_35_to_39_years`+`Total_Male_40_to_44_years`)/Total,
                     
                     prop_age_45_54 = (`Total_Female_45_to_49_years`+`Total_Female_50_to_54_years`+
                                         `Total_Male_45_to_49_years`+`Total_Male_50_to_54_years`)/Total,
                     
                     prop_age_55_64 = (`Total_Female_55_to_59_years`+`Total_Female_60_and_61_years`+
                                         `Total_Female_62_to_64_years`+
                                         `Total_Male_55_to_59_years`+`Total_Male_60_and_61_years`+
                                         `Total_Male_62_to_64_years`) / Total,
                     prop_age_65_74 =  (`Total_Female_65_and_66_years` + `Total_Female_67_to_69_years` +
                                          `Total_Male_65_and_66_years` + `Total_Male_67_to_69_years` +
                                          `Total_Female_70_to_74_years` +`Total_Male_70_to_74_years`) / Total,
                     prop_female = Total_Female/Total) %>%
  
  dplyr::select (fips, prop_age_under_18, prop_age_18_29, prop_age_50_64,  prop_age_65_69, prop_age_18_24:prop_female)



fips_education2 <- dplyr::mutate (fips_education, 
                           prop_college_educated = (`Total_Bachelors_degree` + 
                                                    `Total_Masters_degree` +   
                                                    `Total_Doctorate_degree` + 
                                                    `Total_Professional_school_degree`)/Total) %>%
  
  dplyr::select (fips, prop_college_educated)

fips_hispanic_race2 <- dplyr::select (fips_hispanic_race, fips, Total_Hispanic_or_Latino:Total_Hispanic_or_Latino_White_alone)

# Adjust taking out hispanic out of each major category

fips_race2 <- left_join (fips_race, fips_hispanic_race2, by='fips') %>%
  dplyr::mutate (race_pct_white = (Total_White_alone - Total_Hispanic_or_Latino_White_alone)/Total,
          race_pct_african_american = (Total_Black_or_African_American_alone-Total_Hispanic_or_Latino_Black_or_African_American_alone)/Total,
          race_pct_hispanic = Total_Hispanic_or_Latino/Total,
          race_pct_asian = (Total_Asian_alone-Total_Hispanic_or_Latino_Asian_alone)/Total,
          race_pct_nativeam = (Total_American_Indian_and_Alaska_Native_alone-Total_Hispanic_or_Latino_American_Indian_and_Alaska_Native_alone)/Total,
          race_pct_other = round(1-race_pct_white-race_pct_african_american-race_pct_hispanic-race_pct_asian-race_pct_nativeam,3)) %>%
          dplyr::select(fips, race_pct_white:race_pct_other)

fips_college_enrolled2 <- dplyr::mutate (fips_college_enrolled,
          pct_college_enrolled= (Total_Enrolled_in_school_Enrolled_in_college_undergraduate_years+
                                 Total_Enrolled_in_school_Graduate_or_professional_school) /
                                Total) %>%
            dplyr::select (fips, pct_college_enrolled)

fips_marital2 <- dplyr::mutate (fips_marital, pct_married = 
                           (Total_Female_Now_married_Married_spouse_present +
                            Total_Male_Now_married_Married_spouse_present) /
                           Total) %>%
  dplyr::select (fips, pct_married)

fips_housing2 <- dplyr::mutate (fips_housing,
                         pct_home_ownership = Total_Owner_occupied/Total) %>%
  dplyr::select (fips, pct_home_ownership)
                              
fips_vehicles2 <- dplyr::mutate (fips_vehicles,
                          avg_vehicles_per_hh = 
                            (Total_1_vehicle_available +
                            2*Total_2_vehicles_available +
                            3*Total_3_vehicles_available +
                            4*Total_4_or_more_vehicles_available) / Total,
                          pct_hh_vehicle_ownership=
                            (Total_1_vehicle_available +
                            Total_2_vehicles_available +
                            Total_3_vehicles_available +
                            Total_4_or_more_vehicles_available)/Total) %>%
  dplyr::select (fips, pct_hh_vehicle_ownership, avg_vehicles_per_hh)
                          
fips_military2 <- dplyr::mutate (fips_military,
                          pct_military_employed = Total_Military_specific_occupations/Total ) %>%
  dplyr::select (fips, pct_military_employed)

fips_internet2 <- dplyr::mutate (fips_internet,
                          pct_hh_internet_subscription = 
                            Total_With_an_Internet_subscription/Total) %>%
                  dplyr::select (fips, pct_hh_internet_subscription)

fips_qtrs2 <- dplyr::mutate (fips_qtrs, pop_group_qtrs=Total) %>%
  dplyr::select (fips, pop_group_qtrs)

fips_income_bands2 <- dplyr::mutate (fips_income_bands,
                                     pct_hh_under50k=
                                       (Total_Less_than_10000+
                                       Total_10000_to_14999+
                                       Total_15000_to_19999+
                                       Total_20000_to_24999+
                                       Total_25000_to_29999+
                                       Total_30000_to_34999+
                                       Total_35000_to_39999+
                                       Total_40000_to_44999+
                                       Total_45000_to_49999)/Total) %>%
                       dplyr::select (fips, pct_hh_under50k)



# Join all of these tables on fips

combined_dataset <- fips_pop2 %>%
                      left_join (fips_income2, by='fips') %>%
                      left_join (fips_age2, by='fips') %>%
                      left_join (fips_foodstamps2, by='fips') %>%
                      left_join (fips_housing2, by='fips') %>%
                      left_join (fips_vehicles2, by='fips') %>%
                      left_join (fips_college_enrolled2, by='fips') %>%
                      left_join (fips_education2, by='fips') %>%
                      left_join (fips_marital2, by='fips') %>%
                      left_join (fips_military2, by='fips') %>%
                      left_join (fips_race2, by='fips') %>%
                      left_join (fips_internet2, by='fips') %>%
                      left_join (fips_qtrs2, by='fips') %>%
                      left_join (fips_income_bands2, by='fips') %>%
                      dplyr::mutate (pct_tract_groupqtrs=pop_group_qtrs/population) %>%
                      dplyr::select (-pop_group_qtrs)
  
# Fips preceded with 15000US are census block groups

bg_dataset <- combined_dataset %>%
              dplyr::filter (grepl ('15000US', fips)==TRUE) %>%
              dplyr::mutate (fips=sub("15000US","", fips)) %>%
              dplyr::mutate (census_tract = substr(fips, 1,11),
                      county_fips = substr(fips,1,5)) %>%
  
              dplyr::filter (population>0)

# Fips preceded with 14000US are summaries of multiple block groups in a census tract

tract_dataset <- combined_dataset %>%
  filter (grepl ('14000US', fips)==TRUE) %>%
  mutate (fips=sub("14000US","", fips)) %>%
  mutate (census_tract = fips,
          county_fips = substr(fips,1,5))


# GO through all the nulls at the blockgroup level and fill in
# tract level 

# Columns 4 through 29 are the census column variables

for (j in 4:32) {
  
  column_nulls <- which (is.na(bg_dataset[,j]))
  array_of_tracts <- bg_dataset$census_tract[column_nulls]
  
  # Some columns have 0 nulls
  
  if (length(column_nulls) > 0 ) {
  
  # Loop through nulls
  for (i in 1:length(array_of_tracts)) {
      temp_tract <- array_of_tracts[i]
      row_of_tract <- which(tract_dataset$census_tract==temp_tract)
      # get the right column of the tract and extract the value
      temp_value <- tract_dataset[,j][row_of_tract,][[1]]
      # Fill in correct value in bg_dataset
      bg_dataset[column_nulls[i],j] <- temp_value
      if (i%%10000==0) {print(paste(i,"of",length(array_of_tracts)))}
    }
  }
print(j)
}
############################
save(bg_dataset, file='/mnt/bg_dataset_from_acs.rdA')
#############################

load ('/mnt/bg_dataset_from_acs.rdA')


# Join with Census Planning Database
# https://www.census.gov/topics/research/guidance/planning-databases.2021.html
# Or google "census planning database"
# File with blockgroup level data, joined with data only available on 10-year census
# such as institutionalized vs. non-instituionalized group quarters population
# Also includes land area (in square miles) for each block group\
# Documentation of columns: https://www2.census.gov/adrm/PDB/2021/2021_Block_Group_PDB_Documentation.pdf

census_planning_db <- read_csv("/domino/datasets/local/census_blockgroup_planningdatabase_2021/pdb2021bgv3_us.csv") %>%
                      select (GIDBG, State, LAND_AREA,AIAN_LAND,
                              Tot_Population_CEN_2010,
                              Median_Age_ACS_15_19,
                              Tot_GQ_CEN_2010, Inst_GQ_CEN_2010,
                              Non_Inst_GQ_CEN_2010) %>%
                      mutate (pct_inst_groupquarters_2010=Inst_GQ_CEN_2010/Tot_Population_CEN_2010,
                              pct_non_inst_groupquarters_2010 = Non_Inst_GQ_CEN_2010/Tot_Population_CEN_2010) %>%
                      select (-Tot_Population_CEN_2010,
                              -Tot_GQ_CEN_2010,
                              -Inst_GQ_CEN_2010, -State) %>%
                      filter (is.na(LAND_AREA)==FALSE) %>%
                      mutate (LAND_AREA=ifelse(LAND_AREA==0,.002,LAND_AREA))

bg_dataset_expanded <- 
  left_join (bg_dataset, census_planning_db, by=c('fips'='GIDBG')) %>%
  mutate (pop_density_sqmile = population/LAND_AREA,
          non_institutionized_pop = round(population * (1-pct_inst_groupquarters_2010),0)) %>%
  mutate (military_base=ifelse(pct_military_employed>.1 & pct_non_inst_groupquarters_2010>.1,1,0))
                    
bg_dataset_expanded$military_base[is.na(bg_dataset_expanded$military_base)]<-0

blockgroup_latlon_cap2020 <- read_csv("/domino/datasets/blockgroup_data_latlon_cap2020/blockgroup_data.csv", 
                                      col_types = cols(BGID = col_character())) %>%
  mutate (fips=ifelse(nchar(BGID)==12,BGID,paste0("0",BGID))) %>%
  select (-BGID, -POP20)

# Get tract population density, in case block group is too noisy:

tract_pop_density <- group_by (bg_dataset_expanded, census_tract) %>%
                     summarize (tract_pop = sum(population),
                                tract_sqmile = sum(LAND_AREA)) %>%
                     mutate (tract_pop_density_sqmile=tract_pop/tract_sqmile) %>%
                     select (census_tract, tract_pop_density_sqmile) %>%
                     ungroup()

# Now load in MSA and zipcode columns linked to tracts/counties for joining
# other data sets

# Census tract to zip crosswalk
# From HUD
# https://www.huduser.gov/portal/datasets/usps_crosswalk.html
#

tract_to_zip <- read_csv ('/mnt/tract_to_zip_crosswalk_0621.csv',
                          col_types = cols(zip=col_character())) %>%
                select (tract, zip)

# Get zip with highest percentage of residences

tract_to_zip_deduped <- group_by (tract_to_zip, tract) %>%
                        arrange (tract, -res_ratio) %>%
                        slice(1) %>%
                        ungroup() %>%
                        select (tract, zip)

county_to_msa <- read_csv ('/mnt/county_to_msa_crosswalk.csv') %>%
                 filter (is.na(`MSA Title`)==FALSE & `MSA Type`=='Metro') %>%
                 select(`County Code`,`MSA Title`)



blockgroup_final <- left_join (bg_dataset_expanded, blockgroup_latlon_cap2020, by='fips') %>%
                    left_join (tract_pop_density, by='census_tract') %>%
                    left_join (tract_to_zip_deduped, by=c('census_tract'='tract')) %>%
                    left_join (county_to_msa, by=c('county_fips'='County Code')) %>%
                    rename (bg_fips = fips, census_tract_fips = census_tract,
                            amindian_aknative_hawaiiannative_land_flag=AIAN_LAND,
                            land_area_sqmile=LAND_AREA,
                            median_age = Median_Age_ACS_15_19,
                            pct_tract_military_employed=pct_military_employed,
                            military_base_flag=military_base,
                            CAP_crimescore_2020=CAP20,
                            CAP_personal_crime_2020=PER20,
                            CAP_property_crime_2020=PRO20,
                            MSA = `MSA Title`) %>%
                  select (bg_fips, census_tract_fips, county_fips,
                            state, MSA, zip, blockgroup_center_lat, blockgroup_center_lng,
                            population, non_institutionized_pop,
                            households, pop_density_sqmile, tract_pop_density_sqmile,
                            median_income,
                            median_age,
                            prop_age_under_18:prop_age_55_64,
                            prop_female, pct_hh_foodstamps:pct_tract_groupqtrs,
                            amindian_aknative_hawaiiannative_land_flag,
                            pct_inst_groupquarters_2010,
                            pct_non_inst_groupquarters_2010,
                            military_base_flag,
                            CAP_crimescore_2020:CAP_personal_crime_2020)
names(blockgroup_final)

###############################################
######## RUN unemployment.R to get unemployment
###############################################

unemp_2019 <- filter (bls_unemployment_monthly, year==2019) %>%
  group_by (fips) %>%
  summarize (unemp_2019 = mean(unemployment_rate)) %>%
  rename (county_fips=fips)


blockgroup_final2 <- mutate (blockgroup_final, 
                             county_fips = substr(fips,1,5),
                             census_tract_fips = substr(fips,1,11)) %>%
  left_join (unemp_2019, by='county_fips') %>%
  select(fips, county_fips, census_tract_fips, state.x, population, median_income,
         prop_hh_foodstamps:POP20, unemp_2019,
         blockgroup_center_lng,
         blockgroup_center_lat)

# fill in NAs with census tract avg

census_tract_avgs <- group_by (blockgroup_final2, census_tract_fips) %>%
  summarize (avg_med_inc = mean(median_income, na.rm=TRUE),
             avg_foodstamp = mean(prop_hh_foodstamps, na.rm=TRUE),
             avg_college = mean(prop_college_educated, na.rm=TRUE),
             avg_age1829 = mean(prop_age_18_29, na.rm=TRUE),
             avg_age5064 = mean (prop_age_50_64, na.rm=TRUE),
             avg_pop20 = mean(population,na.rm=TRUE),
             avg_transp = mean(pct_public_trans_towork, na.rm=TRUE),
             blockgroup_center_lng = mean (blockgroup_center_lng, na.rm=TRUE),
             blockgroup_center_lat = mean (blockgroup_center_lat, na.rm=TRUE))

county_avg_inc <- group_by (blockgroup_final2, county_fips) %>%
  summarize (county_inc = median(median_income, na.rm=TRUE),
             county_foodstamp = median(prop_hh_foodstamps,na.rm=TRUE),
             county_transp = median(pct_public_trans_towork, na.rm=TRUE))




blockgroup_facts_dataset <- blockgroup_final2