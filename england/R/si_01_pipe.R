## =============================================================================
## Pipeline (1): Control center, calls relevant analysis scripts, sets working 
## and saving directories, parallelises processes
##
## Author: Samantha Ip
## =============================================================================
# For the fully-adjusted model without backward-selection, sinply:
# Set model parameters, directory paths etc in si_01_pipe and si_02_pipe
# Set mdl <- mdl3b_fullyadj


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
library(parallel)
library(multcomp)

detectCores()


# con <- dbConnect(odbc::odbc(), "Databricks", timeout=60, PWD=rstudioapi::askForPassword("enter databricks personal access token:"))
rm(list=setdiff(ls(), c("con")))
gc()

# specify model 
mdl <- "mdl3b_fullyadj" # "mdl1_unadj", "mdl2_agesex", "mdl3a_bkwdselect", "mdl3b_fullyadj", "mdl4_fullinteract_suppl34", "mdl5_anydiag_death28days", "mdl4_fullinteract_suppl34"
# specify results directory -- I use res_dir/res_dir_date (of course the latter can be not a date)
res_dir_proj <- "~/CCU002_01/results_infection"
res_dir_date <- "2021-08-07"
# specify path to scripts' directory
scripts_dir <- "~/CCU002_01/scripts_si"

# specify model parameters, data source, outcomes of interest, read in relevant data, get prepped covariates
source(file.path(scripts_dir, "si_02_pipe.R"))


gc()


# -----------SET MODEL-SPECIFIC RESULTS DIR  & CALLS MODEL SCRIPT --------------
if (mdl == "mdl1_unadj"){
  res_dir <- file.path(res_dir_proj, res_dir_date, "unadj_nosexforcombined")
  source(file.path(scripts_dir,"si_call_mdl1_unadj.R"))
} else if (mdl == "mdl2_agesex"){
  res_dir <- file.path(res_dir_proj, res_dir_date, "adj_age_sex_only")
  source(file.path(scripts_dir,"si_call_mdl3b_fullyadj.R"))
} else if (mdl == "mdl3a_bkwdselect"){
  res_dir <- file.path(res_dir_proj, res_dir_date, "fully_adj_bkwdselect")
  source(file.path(scripts_dir,"si_call_mdl3a_bkwdselect.R"))
} else if (mdl == "mdl3b_fullyadj"){
  res_dir <- file.path(res_dir_proj, res_dir_date, "fully_adj_bkwdselect")
  source(file.path(scripts_dir,"si_call_mdl3b_fullyadj.R"))
} else if (mdl == "mdl4_fullinteract_suppl34"){
  res_dir <- file.path(res_dir_proj, res_dir_date, "interactionterm")
  source(file.path(scripts_dir,"si_call_mdl4_fullinteract_interactionterm.R"))
} else if ((mdl == "mdl5_anydiag_death28days") & (data_version == "death28days")){
  res_dir <- file.path(res_dir_proj, res_dir_date, "fully_adj_death28days")
  source(file.path(scripts_dir,"si_call_mdl3b_fullyadj.R"))
} else if ((mdl == "mdl5_anydiag_death28days") & (data_version == "anydiag")){
  res_dir <- file.path(res_dir_proj, res_dir_date, "fully_adj_anydiag")
  source(file.path(scripts_dir,"si_call_mdl3b_fullyadj.R"))
}
# creates if does not exist and sets working directory
dir.create(file.path(res_dir), recursive =TRUE)
setwd(file.path(res_dir))




# ------------- DETERMINE WHICH COMBOS HAVE NOT BEEN COMPLETED -----------------
if (mdl == "mdl4_fullinteract_suppl34"){
  ls_interacting_feats <- c("age_deci", "SEX", "CATEGORISED_ETHNICITY", "IMD", 
                            "EVER_TCP", "EVER_THROMBOPHILIA", "EVER_PE_VT", "COVID_infection", 
                            "COCP_MEDS", "HRT_MEDS", "ANTICOAG_MEDS", "ANTIPLATLET_MEDS", 
                            "prior_ami_stroke", "EVER_DIAB_DIAG_OR_MEDS")
  
  interactingfeat_age_vac_combos <- expand.grid(ls_interacting_feats, c("Venous_event", "Arterial_event"))
  names(interactingfeat_age_vac_combos) <- c("interacting_feat", "event", "vac")
  ls_should_have <- pmap(list(interactingfeat_age_vac_combos$interacting_feat, interactingfeat_age_vac_combos$event, interactingfeat_age_vac_combos$vac), 
                         function(interacting_feat, event, vac) 
                           file.path(res_dir,
                                  "wald_",
                                  interacting_feat, "_",
                                  event, ".csv"
                           ))
  
  ls_should_have <- unlist(ls_should_have)
  
  ls_events_missing <- data.frame()
  
  for (i in 1:nrow(interactingfeat_age_vac_combos)) {
    row <- interactingfeat_age_vac_combos[i,]
    fpath <- file.path(res_dir,
                    "wald_",
                    row$interacting_feat, "_",
                    row$event, ".csv")
    
    if (!file.exists(fpath)) {
      ls_events_missing <- rbind(ls_events_missing, row)
    }
  }
} else {
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
  }



ls_events_missing %>% View()
# ls_events_missing <- ls_events_missing %>% filter(! event %in% c("death"))


# ------------------------------------ LAUNCH JOBS -----------------------------
if (mdl == "mdl4_fullinteract_suppl34"){
  ls_event_vac_missing <- unique(ls_events_missing %>% dplyr::select(event,vac))
  
  mclapply(split(ls_event_vac_missing,seq(nrow(ls_event_vac_missing))), mc.cores = 4,
  # lapply(split(ls_event_vac_missing,seq(nrow(ls_event_vac_missing))),
                 function(ls_event_vac_missing)
                   get_vacc_res(
                     ls_interacting_feats, 
                     event=ls_event_vac_missing$event, vac_str=ls_event_vac_missing$vac, 
                     cohort_vac, covars, cuts_weeks_since_expo, master_df_fpath, 
                     cohort_start_date, cohort_end_date, noncase_frac=0.1))
  
} else if (mdl %in% c("mdl3b_fullyadj", "mdl5_anydiag_death28days")) {
   mclapply(split(ls_events_missing,seq(nrow(ls_events_missing))), mc.cores = 2,
   #lapply(split(ls_events_missing,seq(nrow(ls_events_missing))),
           function(ls_events_missing) 
             get_vacc_res(
               sex_as_interaction=FALSE,
               event=ls_events_missing$event, 
               agegp=ls_events_missing$agegp, 
               cohort_vac, covars)
           )
} else if(mdl %in% c("mdl1_unadj", "mdl2_agesex")){
  mclapply(split(ls_events_missing,seq(nrow(ls_events_missing))), mc.cores = 2,
    # lapply(split(ls_events_missing,seq(nrow(ls_events_missing))),
           function(ls_events_missing) 
             get_vacc_res(
               sex_as_interaction=FALSE,
               event=ls_events_missing$event, 
               agegp=ls_events_missing$agegp, 
               cohort_vac, covars)
    )
} else if (mdl == "mdl3a_bkwdselect"){
  print("...... lapply completed ......")
}
