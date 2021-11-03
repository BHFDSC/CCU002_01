# ******************************************************************************
# SAIL project: WMCC - Wales Multi-morbidity Cardiovascular COVID-19 UK
#               CCU002-01: SARS-CoV-2 infection and risk of venous thromboembolism and arterial thrombotic events
# About:        Fully adjusted, with COVID phen
#               Reads in analysis-specific data, loads parameters, censoring at appropriate dates

# Author:       Hoda Abbasizanjani, adopted from scripts created by Sam Ip, Rochelle Knight
# ******************************************************************************
rm(list = ls())

library(data.table)
library(dplyr)
library(survival)
library(table1)
library(broom)
library(DBI)
library(ggplot2)
library(nlme)
library(tidyverse)
library(R.utils)
library(lubridate)
library(purrr)

#-------------------------------------------------------------------------------
# specify path to data
master_df_fpath <- "CCU002_DataPreparation/raw/CCU002_01_COHORT_FULL_20210914.csv"

#-------------------------------------------------------------------------------
# specify model 
mdl <- "mdl2_agesex"

#-------------------------------------------------------------------------------
# specify directories
res_dir_proj <- "CCU002_DataPreparation/results_phen"
scripts_dir <- "CCU002_DataPreparation/Scripts"
#-------------------------------------------------------------------------------
# specify events of interest
ls_events <- c("ARTERIAL_EVENT","VENOUS_EVENT")

# ls_events <- c("ICVT_PREGNANCY","ARTERY_DISSECT","ANGINA","OTHER_DVT","DVT_ICVT","DVT_PREGNANCY",
#                "DVT_DVT","FRACTURE","THROMBOCYTOPENIA","LIFE_ARRHYTHMIA","PERICARDITIS","TTP",
#                "MESENTERIC_THROMBUS","DIC","MYOCARDITIS","STROKE_TIA","STROKE_ISCH","OTHER_ARTERIAL_EMBOLISM",
#                "UNSTABLE_ANGINA","PE","AMI","HF","PORTAL_VEIN_THROMBOSIS","CARDIOMYOPATHY","STROKE_SAH_HS",
#                "ARTERIAL_EVENT","VENOUS_EVENT","HAEMATOLOGICAL_EVENT","DVT_EVENT","ICVT_EVENT")

#-------------------------------------------------------------------------------
# specify study parameters
# agebreaks <- c(0, 40, 60, 80, 500)
# agelabels <- c("-40", "40-59", "60-79", "+80")
agebreaks <- c(0, 500)
agelabels <- c("all")
noncase_frac <- 0.1

cohort_start_date <- as.Date("2020-01-01")
cohort_end_date <- as.Date("2020-12-07")

cuts_weeks_since_expo <- c(1, 2, 4, 8, 12, 26, as.numeric(ceiling(difftime(cohort_end_date,cohort_start_date)/7))) 
cuts_weeks_since_expo_reduced <- c(4, as.numeric(ceiling(difftime(cohort_end_date,cohort_start_date)/7))) 

expo <- "INFECTION"

#-------------------------------------------------------------------------------
# inspect column names of dataset
master_names <- fread(master_df_fpath, nrows=1)
sort(names(master_names))

#===============================================================================
#  READ IN DATA
#-------------------------------------------------------------------------------
cohort_vac_cols <- c("ALF_E",
                     "COV_SEX",
                     "DEATH_DATE",
                     "COV_AGE",
                     "EXP_CONFIRMED_COVID19_DATE",
                     "EXP_CONFIRMED_COVID_PHENOTYPE"
)

cohort_vac <- data.table::fread(master_df_fpath, select=cohort_vac_cols)

setnames(cohort_vac,
         old = c("DEATH_DATE",
                 "COV_SEX",
                 "COV_AGE",
                 "EXP_CONFIRMED_COVID19_DATE",
                 "EXP_CONFIRMED_COVID_PHENOTYPE"
         ),
         new = c("DATE_OF_DEATH",
                 "SEX",
                 "AGE_AT_COHORT_START",
                 "EXPO_DATE",
                 "EXPO_TYPE"
         ))

print(head(cohort_vac))

#-------------------------------------------------------------------------------
# prepare covariates
covar_names <-    c("ALF_E",
                    "COV_ETHNICITY",
                    "COV_SMOKING_STATUS",
                    "COV_EVER_AMI",
                    "COV_EVER_PE_VT",
                    "COV_EVER_ICVT",
                    "COV_EVER_ALL_STROKE",
                    "COV_EVER_THROMBOPHILIA",
                    "COV_EVER_TCP",
                    "COV_EVER_DEMENTIA",
                    "COV_EVER_LIVER_DISEASE",
                    "COV_EVER_CKD",
                    "COV_EVER_CANCER",
                    "COV_SURGERY_LASTYR",
                    "COV_EVER_HYPERTENSION",
                    "COV_EVER_DIABETES",
                    "COV_EVER_OBESITY",
                    "COV_EVER_DEPRESSION",
                    "COV_EVER_COPD",
                    "COV_DEPRIVATION",
                    "COV_ANTIPLATELET_MEDS",
                    "COV_LIPID_MEDS",
                    "COV_ANTICOAGULATION_MEDS",
                    "COV_COCP_MEDS",
                    "COV_HRT_MEDS",
                    "COV_N_DISORDER",
                    "COV_EVER_OTHER_ARTERIAL_EMBOLISM",
                    "COV_EVER_DIC",
                    "COV_EVER_MESENTERIC_THROMBUS",
                    "COV_EVER_ARTERY_DISSECT",
                    "COV_EVER_LIFE_ARRHYTHMIA",
                    "COV_EVER_CARDIOMYOPATHY",
                    "COV_EVER_HF",
                    "COV_EVER_PERICARDITIS",
                    "COV_EVER_MYOCARDITIS",
                    "COV_UNIQUE_BNF_CHAPS"
)

covars <- fread(master_df_fpath, select = covar_names)

# wrangles covariates to the correct data types, deal with NAs and missing values, set reference levels
source(file.path(scripts_dir, "prep_covariates.R"))

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
}

#============================ GET VACCINE-SPECIFIC DATASET =====================
get_pheno_specific_dataset <- function(survival_data, pheno_of_interest){
  survival_data$DATE_EXPO_CENSOR <- as.Date(ifelse(!(survival_data$EXPO_TYPE %in% pheno_of_interest), survival_data$expo_date, NA),
                                            origin='1970-01-01')
  
  
  survival_data$expo_date <- as.Date(ifelse((!is.na(survival_data$DATE_EXPO_CENSOR)) & (survival_data$expo_date >= survival_data$DATE_EXPO_CENSOR), NA, survival_data$expo_date), origin='1970-01-01')
  survival_data$record_date <- as.Date(ifelse((!is.na(survival_data$DATE_EXPO_CENSOR)) & (survival_data$record_date >= survival_data$DATE_EXPO_CENSOR), NA, survival_data$record_date), origin='1970-01-01')
  
  cat(paste("pheno-specific df: should see expos other than", paste(pheno_of_interest, collapse = "|"), "as DATE_EXPO_CENSOR ... \n", sep="..."))
  print(head(survival_data, 30 ))
  
  cat(paste("min-max expo_date: ", min(survival_data$expo_date, na.rm=TRUE), max(survival_data$expo_date, na.rm=TRUE), "\n", sep="   "))
  cat(paste("min-max record_date: ", min(survival_data$record_date, na.rm=TRUE), max(survival_data$record_date, na.rm=TRUE), "\n", sep="   "))
  
  return(survival_data)
}
#-------------------------------------------------------------------------------
# set model specific results dir & calls model script
res_dir <- file.path(res_dir_proj, "adj_age_sex_only")
source(file.path(scripts_dir,"call_mdl3b_fullyadj_covidpheno.R"))

#-------------------------------------------------------------------------------
# determine which compositions have not been completed
outcome_age_vac_combos <- expand.grid(ls_events, agelabels, c("non-hospitalised", "hospitalised"))

names(outcome_age_vac_combos) <- c("event", "agegp", "vac")

ls_should_have <- pmap(list(outcome_age_vac_combos$event, outcome_age_vac_combos$agegp, outcome_age_vac_combos$vac), 
                       function(event, agegp, vac)
                         paste0(res_dir,
                                "tbl_hr_INFECTION_",
                                event, "_",
                                agegp, "_",
                                vac, ".csv"
                         ))

ls_should_have <- unlist(ls_should_have)

ls_events_missing <- data.frame()

for (i in 1:nrow(outcome_age_vac_combos)) {
  row <- outcome_age_vac_combos[i,]
  fpath <- file.path(res_dir,
                     paste0("tbl_hr_INFECTION_",
                            row$event, "_",
                            row$agegp, "_",
                            row$vac, ".csv"))
  
  
  if (!file.exists(fpath)) {
    ls_events_missing <- rbind(ls_events_missing, row)
  }
}

ls_events_missing %>% View()
#-------------------------------------------------------------------------------
# lunch jobs
  lapply(split(ls_events_missing,seq(nrow(ls_events_missing))),
           function(ls_events_missing) 
             get_vacc_res(
               sex_as_interaction=FALSE,
               event=ls_events_missing$event,
               pheno_str=ls_events_missing$vac,
               agegp=ls_events_missing$agegp, 
               cohort_vac, covars)
  )
