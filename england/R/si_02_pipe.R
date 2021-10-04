## =============================================================================
## Pipeline (2): Reads in analysis-specific data, loads parameters, 
## gets vaccine-specific dataset -- censoring at appropriate dates
##
## Author: Samantha Ip
## =============================================================================
# specify events of interest
ls_events <- c("artery_dissect","angina","other_DVT","fracture")

# specify path to data
master_df_fpath <- "~/CCU002_01/ccu002_01_cohort_all_outcomes_20210807.csv.gz"

# specify study parameters
agebreaks <- c(0, 40, 60, 80, 500)
agelabels <- c("<40", "40-59", "60-79", ">=80")
noncase_frac <- 0.1


cohort_start_date <- as.Date("2020-01-01")
cohort_end_date <- as.Date("2020-12-07")

cuts_weeks_since_expo <- c(1, 2, 4, 8, 12, 26, as.numeric(ceiling(difftime(cohort_end_date,cohort_start_date)/7))) 
cuts_weeks_since_expo_reduced <- c(4, as.numeric(ceiling(difftime(cohort_end_date,cohort_start_date)/7))) 


expo <- "INFECTION"



# inspect column names of dataset
master_names <- fread(master_df_fpath, nrows=1)
sort(names(master_names))


# ---------------------- READ IN DATA ------------------------------------------
# read in core analysis information -- for not fully-adjusted analyses (e.g. "mdl1_unadj", "mdl2_agesex") and exploration (save memory)
cohort_vac_cols <- c("NHS_NUMBER_DEID", 
                     "cov_sex", 
                     "death_date", 
                     "cov_age", 
                     "exp_confirmed_covid19_date", 
                     "cov_region")

cohort_vac <- fread(master_df_fpath, 
                    select=cohort_vac_cols)

setnames(cohort_vac, 
         old = c("death_date",  
                 "cov_sex", 
                 "cov_age", 
                 "exp_confirmed_covid19_date", 
                 "cov_region"), 
         new = c("DATE_OF_DEATH", 
                 "SEX", 
                 "AGE_AT_COHORT_START", 
                 "EXPO_DATE", 
                 "region_name"))

print(head(cohort_vac))
gc()

if (! mdl %in% c("mdl1_unadj", "mdl2_agesex")){
  covar_names <- master_names %>% dplyr::select(!c(all_of(cohort_vac_cols[cohort_vac_cols != "NHS_NUMBER_DEID"]), 
                                                   "CHUNK", "exp_confirmed_covid_phenotype",
                                                   names(master_names)[grepl("^out_", names(master_names))]
  )) %>% names()
  
  covars <- fread(master_df_fpath, select = covar_names)
  gc()
  
  # wrangles covariates to the correct data types, deal with NAs and missing values, set reference levels
  source(file.path(scripts_dir, "si_prep_covariates.R"))
} else {
  covars <- cohort_vac %>% dplyr::select(NHS_NUMBER_DEID)
}

#Now have combined cov_ever_arterial_event and cov_ever_venous_event so need to remove covariates contained within these
covars = subset(covars, select = -c(cov_ever_ami, cov_ever_other_arterial_embolism, cov_ever_pe_vt, cov_ever_icvt ))
covar_names = covar_names[!covar_names %in% c(cov_ever_ami, cov_ever_other_arterial_embolism, cov_ever_pe_vt, cov_ever_icvt )]

#------------------------ SET DATES OUTSIDE RANGE AS NA ------------------------
set_dates_outofrange_na <- function(df, colname)
{
  df <- df %>% mutate(
    !!sym(colname) := as.Date(ifelse((!!sym(colname) > cohort_end_date) | (!!sym(colname) < cohort_start_date), NA, !!sym(colname) ), origin='1970-01-01')
  )
  return(df)
}


#------------------------ RM GLOBAL OBJ FROM WITHIN FN -------------------------
rm_from_within_fn <- function(obj_name) {
  objs <- ls(pos = ".GlobalEnv")
  rm(list = objs[grep(obj_name, objs)], pos = ".GlobalEnv")
  gc()
}



