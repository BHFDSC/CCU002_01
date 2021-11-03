# ******************************************************************************
# SAIL project: WMCC - Wales Multi-morbidity Cardiovascular COVID-19 UK
#               CCU002-01: SARS-CoV-2 infection and risk of venous thromboembolism and arterial thrombotic events
# About:        Unadjusted analysis 
#               Reads in analysis-specific data, loads parameters, censoring at appropriate dates

# Author:       Hoda Abbasizanjani, adopted from scripts created by Sam Ip, Rochelle Knight
# ******************************************************************************
rm(list = ls())

library(magrittr)
library(data.table)
library(dplyr)
library(survival)
library(table1)
library(broom)
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
res_dir_proj <- "CCU002_DataPreparation/results"
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
master_names <- data.table::fread(master_df_fpath, nrows=1)
sort(names(master_names))

#-------------------------------------------------------------------------------
# read in core analysis information
cohort_vac_cols <- c("ALF_E",
                     "COV_SEX",
                     "DEATH_DATE",
                     "COV_AGE",
                     "EXP_CONFIRMED_COVID19_DATE"
                     )

cohort_vac <- data.table::fread(master_df_fpath, select=cohort_vac_cols)

setnames(cohort_vac,
         old = c("DEATH_DATE",
                 "COV_SEX",
                 "COV_AGE",
                 "EXP_CONFIRMED_COVID19_DATE"
                 ),
         new = c("DATE_OF_DEATH",
                 "SEX",
                 "AGE_AT_COHORT_START",
                 "EXPO_DATE"
                 ))

print(head(cohort_vac))

#-------------------------------------------------------------------------------
# prepare covariates
covars <- cohort_vac %>% dplyr::select(ALF_E)

#-------------------------------------------------------------------------------
# set dates outside range as NA
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

#-------------------------------------------------------------------------------
# set model specific results dir & calls model script
res_dir <- file.path(res_dir_proj, "unadj_nosexforcombined")
source(file.path(scripts_dir,"call_mdl1_unadj.R"))

#-------------------------------------------------------------------------------
# determine which compositions have not been completed
  outcome_age_combos <- expand.grid(ls_events, agelabels)
  names(outcome_age_combos) <- c("event", "agegp")
  ls_should_have <- pmap(list(outcome_age_combos$event, outcome_age_combos$agegp), 
                         function(event, agegp) 
                           file.path(res_dir,
                                     paste0("tbl_hr_INFECTION_",
                                            event, "_",
                                            agegp, ".csv")
                           ))

  ls_should_have <- unlist(ls_should_have)

  ls_events_missing <- data.frame()

  for (i in 1:nrow(outcome_age_combos)) {
    row <- outcome_age_combos[i,]
    fpath <- file.path(res_dir,
                       paste0("tbl_hr_INFECTION_",
                              row$event, "_",
                              row$agegp, ".csv"))
    
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
               agegp=ls_events_missing$agegp,
               cohort_vac, covars)
  )
