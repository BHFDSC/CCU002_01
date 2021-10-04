## =============================================================================
## FORMATS RESULTS from RSTUDIO
##
## Author: Samantha Ip
## =============================================================================

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

detectCores()
rm(list=ls())

#========================== HRs phenotypes ===============================
res_dir_proj <- "~/CCU002_01/results_infection"
res_dir_date <- "2021-08-07"
mdl <- "mdl3b_fullyadj" # "mdl1_unadj", "mdl2_agesex", "mdl3a_bkwdselect", "mdl3b_fullyadj", "mdl4_fullinteract_suppl34", "mdl5_anydiag_death28days", "mdl4_fullinteract_suppl34"
data_version <- "death28days" # death28days, anydiag

if (mdl == "mdl1_unadj"){
  res_dir <- file.path(res_dir_proj, res_dir_date, "unadj_nosexforcombined")
} else if (mdl == "mdl2_agesex"){
  res_dir <- file.path(res_dir_proj, res_dir_date, "adj_age_sex_only")
} else if (mdl == "mdl3a_bkwdselect"){
  res_dir <- file.path(res_dir_proj, res_dir_date, "fully_adj_bkwdselect")
} else if (mdl == "mdl3b_fullyadj"){
  res_dir <- file.path(res_dir_proj, res_dir_date, "fully_adj_bkwdselect")
} else if (mdl == "mdl4_fullinteract_suppl34"){
  res_dir <- file.path(res_dir_proj, res_dir_date, "interactionterm")
} else if ((mdl == "mdl5_anydiag_death28days") & (data_version == "death28days")){
  res_dir <- file.path(res_dir_proj, res_dir_date, "fully_adj_death28days")
} else if ((mdl == "mdl5_anydiag_death28days") & (data_version == "anydiag")){
  res_dir <- file.path(res_dir_proj, res_dir_date, "fully_adj_anydiag")
}
setwd(file.path(res_dir))



ls_events <- c("thrombocytopenia","pericarditis","PE","other_DVT","mesenteric_thrombus","life_arrhythmia","DVT_event","angina","AMI","myocarditis","stroke_TIA","stroke_isch")
print(ls_events)
# ls_events <- ls_events[!ls_events %in% c("DIC", "TTP")]

agelabels <- c("<40", "40-59", "60-79", ">=80")


outcome_age_vac_combos <- expand.grid(ls_events, agelabels, c("hospitalised", "non_hospitalised"))
# outcome_age_vac_combos <- expand.grid(ls_events, agelabels, c("vac_az", "vac_pf", "vac_all"))
names(outcome_age_vac_combos) <- c("event", "agegp", "covidpheno")


ls_hrs <- pmap(list(outcome_age_vac_combos$event, outcome_age_vac_combos$agegp, outcome_age_vac_combos$covidpheno),
               function(event, agegp, covidpheno)
                 file.path(res_dir,
                        paste0("tbl_hr_INFECTION_",
                        event, "_",
                        agegp, "_",
                        covidpheno, ".csv")
                 )
)
ls_should_have <- unlist(ls_hrs)

ls_events_missing <- data.frame()
ls_events_done <- c()
for (i in 1:nrow(outcome_age_vac_combos)) {
  row <- outcome_age_vac_combos[i,]
  fpath <- file.path(res_dir,
                  paste0("tbl_hr_INFECTION_",
                  row$event, "_",
                  row$agegp, "_",
                  row$covidpheno, ".csv"))

  if (!file.exists(fpath)) {
    ls_events_missing <- rbind(ls_events_missing, row)
  } else {
    ls_events_done <- c(ls_events_done, fpath)
  }
}

# which ones are missing?
ls_events_missing %>% View()
print(ls_events_missing)

#  fread completed ones
# ls_hrs <- lapply(ls_events_done, fread)

outcome_age_vac_combos <- anti_join(outcome_age_vac_combos, ls_events_missing)


ls_hrs <- pmap(list(ls_events_done, outcome_age_vac_combos$covidpheno), 
               function(fpath, covidpheno){ 
                 df <- fread(fpath) 
                 df$covidpheno <- covidpheno
                 return(df)
               })



df_hr <- rbindlist(ls_hrs, fill=TRUE)
df_hr <- df_hr %>% mutate_if(is.numeric, round, digits=5) %>% dplyr::select("event", "covidpheno", "agegp", "sex",
                                                                    "term", "estimate", "conf.low", "conf.high", "p.value", "std.error", "robust.se",
                                                                    "statistic"
)
df_hr

df_hr <- df_hr %>% filter(sex=="all")

write.csv(df_hr, file = file.path(res_dir, "hrs_vac.csv") , row.names=F)


#========================== HRs no pheno ===============================
res_dir_proj <- "~/CCU002_01/results_infection"
res_dir_date <- "2021-08-07"
mdl <- "mdl3b_fullyadj" # "mdl1_unadj", "mdl2_agesex", "mdl3a_bkwdselect", "mdl3b_fullyadj", "mdl4_fullinteract_suppl34", "mdl5_anydiag_death28days", "mdl4_fullinteract_suppl34"
data_version <- "death28days" # death28days, anydiag

if (mdl == "mdl1_unadj"){
  res_dir <- file.path(res_dir_proj, res_dir_date, "unadj_nosexforcombined")
} else if (mdl == "mdl2_agesex"){
  res_dir <- file.path(res_dir_proj, res_dir_date, "adj_age_sex_only")
} else if (mdl == "mdl3a_bkwdselect"){
  res_dir <- file.path(res_dir_proj, res_dir_date, "fully_adj_bkwdselect")
} else if (mdl == "mdl3b_fullyadj"){
  res_dir <- file.path(res_dir_proj, res_dir_date, "fully_adj_bkwdselect")
} else if (mdl == "mdl4_fullinteract_suppl34"){
  res_dir <- file.path(res_dir_proj, res_dir_date, "interactionterm")
} else if ((mdl == "mdl5_anydiag_death28days") & (data_version == "death28days")){
  res_dir <- file.path(res_dir_proj, res_dir_date, "fully_adj_death28days")
} else if ((mdl == "mdl5_anydiag_death28days") & (data_version == "anydiag")){
  res_dir <- file.path(res_dir_proj, res_dir_date, "fully_adj_anydiag")
}
setwd(file.path(res_dir))



ls_events <- c("thrombocytopenia","pericarditis","PE","other_DVT","mesenteric_thrombus","life_arrhythmia","DVT_event","angina","AMI","myocarditis","stroke_TIA","stroke_isch","other_arterial_embolism","Arterial_event","fracture","stroke_SAH_HS","Venous_event","Haematological_event")
print(ls_events)
# ls_events <- ls_events[!ls_events %in% c("DIC", "TTP")]

agelabels <- c("<40", "40-59", "60-79", ">=80")


outcome_age_vac_combos <- expand.grid(ls_events, agelabels)
# outcome_age_vac_combos <- expand.grid(ls_events, agelabels, c("vac_az", "vac_pf", "vac_all"))
names(outcome_age_vac_combos) <- c("event", "agegp")


ls_hrs <- pmap(list(outcome_age_vac_combos$event, outcome_age_vac_combos$agegp),
               function(event, agegp)
                 file.path(res_dir,
                           paste0("tbl_hr_INFECTION_",
                                  event, "_",
                                  agegp, ".csv")
                 )
)
ls_should_have <- unlist(ls_hrs)

ls_events_missing <- data.frame()
ls_events_done <- c()
for (i in 1:nrow(outcome_age_vac_combos)) {
  row <- outcome_age_vac_combos[i,]
  fpath <- file.path(res_dir,
                     paste0("tbl_hr_INFECTION_",
                            row$event, "_",
                            row$agegp, ".csv"))
  
  if (!file.exists(fpath)) {
    ls_events_missing <- rbind(ls_events_missing, row)
  } else {
    ls_events_done <- c(ls_events_done, fpath)
  }
}

# which ones are missing?
ls_events_missing %>% View()
print(ls_events_missing)

#  fread completed ones
# ls_hrs <- lapply(ls_events_done, fread)

outcome_age_vac_combos <- anti_join(outcome_age_vac_combos, ls_events_missing)


ls_hrs <- pmap(list(ls_events_done), 
               function(fpath){ 
                 df <- fread(fpath) 
                 return(df)
               })



df_hr <- rbindlist(ls_hrs, fill=TRUE)
df_hr <- df_hr %>% mutate_if(is.numeric, round, digits=5) %>% dplyr::select("event", "agegp", "sex",
                                                                            "term", "estimate", "conf.low", "conf.high", "p.value", "std.error", "robust.se",
                                                                            "statistic"
)
df_hr

df_hr <- df_hr %>% filter(sex=="all")

write.csv(df_hr, file = file.path(res_dir, "hrs_vac.csv") , row.names=F)


# =============================  R events count COVID PHENO =====================================
ls_hrs <- pmap(list(outcome_age_vac_combos$event, outcome_age_vac_combos$agegp, outcome_age_vac_combos$covidpheno),
               function(event, agegp, covidpheno)
                 file.path(res_dir,
                           paste0("tbl_event_count_",
                                  event, "_",
                                  agegp, "_",
                                  covidpheno,
                                  ".csv")
                 )
)
ls_should_have <- unlist(ls_hrs)

ls_events_missing <- data.frame()
ls_events_done <- c()
for (i in 1:nrow(outcome_age_vac_combos)) {
  row <- outcome_age_vac_combos[i,]
  fpath <- file.path(res_dir,
                     paste0("tbl_event_count_INFECTION_",
                            row$event, "_",
                            row$agegp,  "_",
                            row$covidpheno,
                            ".csv"))
  
  if (!file.exists(fpath)) {
    ls_events_missing <- rbind(ls_events_missing, row)
  } else {
    ls_events_done <- c(ls_events_done, fpath)
  }
}

# which ones are missing?
print(ls_events_missing)

#  fread completed ones
ls_hrs <- pmap(list(ls_events_done, outcome_age_vac_combos$event, outcome_age_vac_combos$agegp, outcome_age_vac_combos$covidpheno), 
               function(fpath, event, agegp, covidpheno){ 
                 df <- fread(fpath) 
                 df$event <- event
                 df$agegp <- agegp
                 df$covidpheno <- covidpheno
                 return(df)
               })



df_hr <- rbindlist(ls_hrs, fill=TRUE)  %>% dplyr::select(!"V1")
df_hr <- df_hr %>% dplyr::select(event, covidpheno, agegp, expo_week, events_total, events_M, events_F)

write.csv(df_hr, file = file.path(res_dir, "event_count.csv") , row.names=F)





# =============================  R events count =====================================
ls_hrs <- pmap(list(outcome_age_vac_combos$event, outcome_age_vac_combos$agegp),
               function(event, agegp)
                 file.path(res_dir,
                           paste0("tbl_event_count_",
                                  event, "_",
                                  agegp, ".csv")
                 )
)
ls_should_have <- unlist(ls_hrs)

ls_events_missing <- data.frame()
ls_events_done <- c()
for (i in 1:nrow(outcome_age_vac_combos)) {
  row <- outcome_age_vac_combos[i,]
  fpath <- file.path(res_dir,
                     paste0("tbl_event_count_INFECTION_",
                            row$event, "_",
                            row$agegp, ".csv"))
  
  if (!file.exists(fpath)) {
    ls_events_missing <- rbind(ls_events_missing, row)
  } else {
    ls_events_done <- c(ls_events_done, fpath)
  }
}

# which ones are missing?
print(ls_events_missing)

#  fread completed ones
ls_hrs <- pmap(list(ls_events_done, outcome_age_vac_combos$event, outcome_age_vac_combos$agegp), 
               function(fpath, event, agegp){ 
                 df <- fread(fpath) 
                 df$event <- event
                 df$agegp <- agegp
                 return(df)
                 })



df_hr <- rbindlist(ls_hrs, fill=TRUE)  %>% dplyr::select(!"V1")
df_hr <- df_hr %>% dplyr::select(event, agegp, expo_week, events_total, events_M, events_F)

write.csv(df_hr, file = file.path(res_dir, "event_count.csv") , row.names=F)





# # ============================= suppl tbl 3 & 4 ================================
# res_dir_date <- "2021-06-15"
# mdl <- "mdl4_fullinteract_suppl34" # mdl3b_fullyadj, mdl4_fullinteract_suppl34, mdl5_anydiag_death28days
# 
# setwd(paste0("/mnt/efs/hyi20/dvt_icvt_results/", res_dir_date, "/interactionterm/"))
# res_dir <- paste0("/mnt/efs/hyi20/dvt_icvt_results/", res_dir_date, "/interactionterm/")
# 
# ls_events <- c("Venous_event", "Arterial_event")
# ls_interacting_feats <- c("age_deci", "SEX", "CATEGORISED_ETHNICITY", "IMD", 
#                           "EVER_TCP", "EVER_THROMBOPHILIA", "EVER_PE_VT", "COVID_infection", 
#                           "COCP_MEDS", "HRT_MEDS", "ANTICOAG_MEDS", "ANTIPLATLET_MEDS", 
#                           "prior_ami_stroke", "EVER_DIAB_DIAG_OR_MEDS")
# 
# interactingfeat_age_vac_combos <- expand.grid(ls_interacting_feats, c("Venous_event", "Arterial_event"), c("vac_az", "vac_pf"))
# names(interactingfeat_age_vac_combos) <- c("interacting_feat", "event", "vac")
# ls_wald <- pmap(list(interactingfeat_age_vac_combos$interacting_feat, interactingfeat_age_vac_combos$event, interactingfeat_age_vac_combos$vac), 
#                        function(interacting_feat, event, vac_str) 
#                          paste0(res_dir,
#                                 "wald_",
#                                 interacting_feat, "_",
#                                 event, "_",
#                                 vac_str, ".csv"
#                          ))
# 
# ls_fit_tidy <- pmap(list(interactingfeat_age_vac_combos$interacting_feat, interactingfeat_age_vac_combos$event, interactingfeat_age_vac_combos$vac), 
#                function(interacting_feat, event, vac_str) 
#                  paste0(res_dir, "fit_ref_wald_tidy_",  interacting_feat,  "_", event,  "_", vac_str, ".csv"))
# 
# ls_fit_glht <- pmap(list(interactingfeat_age_vac_combos$interacting_feat, interactingfeat_age_vac_combos$event, interactingfeat_age_vac_combos$vac), 
#                     function(interacting_feat, event, vac_str) 
#                       paste0(res_dir, "fit_ref_wald_glht_",  interacting_feat,  "_", event,  "_", vac_str, ".csv"))
# 
# ls_wald <- unlist(ls_wald)
# ls_fit_tidy <- unlist(ls_fit_tidy)
# ls_fit_glht <- unlist(ls_fit_glht)
# 
# 
# ls_events_missing <- data.frame()
# ls_events_done <- c()
# for (i in 1:nrow(interactingfeat_age_vac_combos)) {
#   row <- interactingfeat_age_vac_combos[i,]
#   fpath <- paste0(res_dir,
#                   "wald_",
#                   row$interacting_feat, "_",
#                   row$event, "_",
#                   row$vac_str, ".csv"
#   )
#   
#   if (!file.exists(fpath)) {
#     ls_events_missing <- rbind(ls_events_missing, row)
#   } else {
#     ls_events_done <- c(ls_events_done, fpath)
#   }
# }
# 
# # which ones are missing?
# ls_events_missing %>% View()
# print(ls_events_missing)
# 
# #  fread completed ones
# ls_wald <- lapply(ls_wald, fread)
# ls_fit_tidy <- lapply(ls_fit_tidy, fread)
# ls_fit_glht <- lapply(ls_fit_glht, fread)
# 
# # ...... wald ......
# df_wald <- rbindlist(ls_wald, fill=TRUE)
# df_wald <- df_wald %>% filter(! is.na(statistic))
# df_wald <- df_wald %>% mutate_if(is.numeric, round, digits=5) %>% dplyr::select("event", "interacting_feat", "vac_str", "week", 
#                                                                                 "p.value", "df", "res.df")
# write.csv(df_wald, file = paste0(res_dir, "hrs_vac_wald.csv") , row.names=F)
# 
# # ...... fit_tidy ......
# ls_fit_tidy <- map2(split(interactingfeat_age_vac_combos,seq(nrow(interactingfeat_age_vac_combos))), ls_fit_tidy, 
#                     function(feats, df) 
#                       {
#                       df$event <- feats$event
#                       df$interacting_feat <- feats$interacting_feat
#                       df$vac <- feats$vac
#                       return(df)
#                       }
#                     )
# 
# df_fit_tidy <- rbindlist(ls_fit_tidy, fill=TRUE)
# df_fit_tidy <- df_fit_tidy %>% mutate_if(is.numeric, round, digits=5) %>% dplyr::select("event", "interacting_feat", "vac", 
#                                                                             "term", "estimate", "conf.low", "conf.high", "p.value", "std.error", "robust.se", 
#                                                                             "statistic"
#                                                                             )
# write.csv(df_fit_tidy, file = paste0(res_dir, "hrs_vac_fit_tidy.csv") , row.names=F)
# 
# # ...... fit_glht ......
# df_fit_glht <- rbindlist(ls_fit_glht, fill=TRUE)
# df_fit_glht <- df_fit_glht %>% mutate_if(is.numeric, round, digits=5) %>% dplyr::select("event", "interacting_feat", "vac_str",
#                                                                                         "contrast", "estimate", "std.error", "adj.p.value", 
#                                                                                         "statistic", "null.value"
#                                                                                         )
# write.csv(df_fit_glht, file = paste0(res_dir, "hrs_vac_fit_glht.csv") , row.names=F)
