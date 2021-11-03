# ******************************************************************************
# SAIL project: WMCC - Wales Multi-morbidity Cardiovascular COVID-19 UK
#               CCU002-01: SARS-CoV-2 infection and risk of venous thromboembolism and arterial thrombotic events
# About:        Unadjusted analysis with COVID phen
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
mdl <- "mdl1_unadj"

#-------------------------------------------------------------------------------
# specify directories
res_dir_proj <- "CCU002_DataPreparation/results_phen"
scripts_dir <- "CCU002_DataPreparation/Scripts"

#-------------------------------------------------------------------------------
# specify events of interest
ls_events <- c("ARTERIAL_EVENT","VENOUS_EVENT")
# ls_events <- c("ARTERY_DISSECT","ANGINA","OTHER_DVT","TTP","STROKE_SAH_HS","DIC","DVT_ICVT",
#                "DVT_DVT","FRACTURE","THROMBOCYTOPENIA","LIFE_ARRHYTHMIA","PERICARDITIS","PE",
#                "MESENTERIC_THROMBUS","MYOCARDITIS","STROKE_TIA","STROKE_ISCH","HF","HAEMATOLOGICAL_EVENT",
#                "UNSTABLE_ANGINA","AMI","PORTAL_VEIN_THROMBOSIS","CARDIOMYOPATHY","DVT_EVENT",
#                "ARTERIAL_EVENT","VENOUS_EVENT","ICVT_EVENT","OTHER_ARTERIAL_EMBOLISM")

#-------------------------------------------------------------------------------
# specify study parameters
#agebreaks <- c(0, 40, 60, 80, 500)
#agelabels <- c("-40", "40-59", "60-79", "+80")
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
  covars <- cohort_vac %>% dplyr::select(ALF_E)

#-------------------------- SET DATES OUTSIDE RANGE AS NA ----------------------
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

#============================ GET DATASET ======================================
get_pheno_specific_dataset <- function(survival_data, pheno_of_interest){
  survival_data$DATE_EXPO_CENSOR <- as.Date(ifelse(!(survival_data$EXPO_TYPE %in% pheno_of_interest),
                                                  survival_data$expo_date, 
                                                  NA), origin='1970-01-01')


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
res_dir <- file.path(res_dir_proj, "unadj_nosexforcombined")
source(file.path(scripts_dir,"call_mdl1_unadj_covidpheno.R"))

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
