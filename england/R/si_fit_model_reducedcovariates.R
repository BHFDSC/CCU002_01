## =============================================================================
## MODEL3B: FULLY ADJUSTED -- USING PRE-DEFINED OUTSOME-SPECIFIC FIXED COVARIATES 
## and AMI BACKWARD-SELECTED COVARIATES
##
## Author: Samantha Ip
## =============================================================================
source(file.path(scripts_dir,"si_fit_get_data_surv_eventcountbasedtimecuts.R"))

library(multcomp)


rm_lowvar_covars <- function(data_surv){
  df <- data_surv %>% dplyr::select(c( "expo", "event", "region_name",covar_names)) %>% distinct() %>% filter((expo==1) & (event==1))
  df <- df %>%  dplyr::select(!c("expo", "event", 
                                 df %>%  dplyr::select_if(is.numeric) %>% names(),
                                 df %>%  dplyr::select_if(~n_distinct(.)>2) %>% names()
  ))
  summary <- as.data.frame(summary(df))
  summary$Freq <- gsub("0:","",summary$Freq)
  summary$Freq <- gsub("1:","",summary$Freq)
  summary$Freq <- as.numeric(summary$Freq)
  return(as.character(summary$Var2[summary$Freq <=2]))
}




#------------------------ GET SURV FORMULA & COXPH() ---------------------------
coxfit_bkwdselection <- function(data_surv, sex, interval_names, fixed_covars, event, agegp, sex_as_interaction, covar_names){
  cat("...... sex ", sex, " ...... \n")
  # which covariates are backward-selected -- to include? ----
  # AMI_bkwdselected_covars <- readRDS(file=paste0("backwards_names_kept_vac_all_AMI_", agegp, "_sex", sex,".rds"))
  covars_to_remove <- rm_lowvar_covars(data_surv)[!is.na((rm_lowvar_covars(data_surv)))]
  data_surv <- data_surv %>% dplyr::select(!covars_to_remove)
 
  AMI_bkwdselected_covars <- covar_names[covar_names %in% names(data_surv)] %>% sort()
  interval_names_withpre <- c("week_pre", interval_names)
  
  # get Surv formula ----
  if (sex_as_interaction){
    cat("...... sex as interaction ...... \n")
    # ---------------------  *sex as interaction term  -------------------------
    # levels(data_surv$IMD) <- c("Deciles_1_2", "Deciles_3_4", "Deciles_5_6", "Deciles_7_8", "Deciles_9_10", "Deciles_9_10")
    redcovariates_excl_region <- unique(c("age", "age_sq", AMI_bkwdselected_covars, fixed_covars))
    redcovariates_excl_region <- names(data_surv %>%
                                         dplyr::select_if(~names(.) %in% redcovariates_excl_region) %>%
                                         summarise_all(list(~n_distinct(.))) %>%
                                         select_if(. != 1) )
    redcovariates_excl_region <- redcovariates_excl_region[!redcovariates_excl_region %in% c("SEX", interval_names)]
    data_surv$week_pre <- 1*((data_surv$week1_4==0) & (data_surv$week5_39==0))
    data_surv <- one_hot_encode("SEX", data_surv, interval_names_withpre)
    print(data_surv)
    
    fml_red <- paste0(
      "Surv(tstart, tstop, event) ~ week*SEX",
      "+", paste(redcovariates_excl_region, collapse="+"), 
      "+ region_name + cluster(NHS_NUMBER_DEID)")
      # "+ cluster(NHS_NUMBER_DEID) + strata(region_name)")
  } else {
    cat("...... sex as additive ...... \n")
    redcovariates_excl_region <- unique(c("age", "age_sq", interval_names, AMI_bkwdselected_covars, fixed_covars))
    cat("...... redcovariates_excl_region ...... \n")
    print(unlist(redcovariates_excl_region))
    fml_red <- paste0(
      "Surv(tstart, tstop, event) ~ ",
      paste(redcovariates_excl_region, collapse="+"), 
      "+ region_name + cluster(NHS_NUMBER_DEID)")
      # "+ cluster(NHS_NUMBER_DEID) + strata(region_name)")
    if ((sex == "all") & (!"SEX" %in% redcovariates_excl_region)){
      fml_red <- paste(fml_red, "SEX", sep="+")
    }
  }
  
  print(fml_red)
  
  # fit coxph() ----
  system.time(fit_red <- coxph(
    formula = as.formula(fml_red), 
    data = data_surv, weights=data_surv$cox_weights #, singular.ok=TRUE
  ))
  if (sex_as_interaction){
    cat("save fit_red where sex_as_interaction ...... \n")
    saveRDS(fit_red, paste0("fit_", event, "_", agegp, ".rds"))
  }
  
  
  # presentation ----
  fit_tidy <-tidy(fit_red, exponentiate = TRUE, conf.int=TRUE)
  fit_tidy$sex<-c(sex)
  
  gc()
  print(fit_tidy)
  return(fit_tidy)
}

fit_model_reducedcovariates <- function(sex_as_interaction, covars, agegp, event, survival_data){
  list_data_surv_noncase_ids_interval_names <- fit_get_data_surv(covars, agegp, event, survival_data, cuts_weeks_since_expo)
  data_surv <- list_data_surv_noncase_ids_interval_names[[1]]
  noncase_ids <- list_data_surv_noncase_ids_interval_names[[2]]
  interval_names <-list_data_surv_noncase_ids_interval_names[[3]]
  ind_any_zeroeventperiod <- list_data_surv_noncase_ids_interval_names[[4]]
  
  if (ind_any_zeroeventperiod){
    cat("...... COLLAPSING POST-EXPO INTERVALS ......")
    list_data_surv_noncase_ids_interval_names <- fit_get_data_surv(covars, agegp, event, survival_data, cuts_weeks_since_expo=cuts_weeks_since_expo_reduced)
    data_surv <- list_data_surv_noncase_ids_interval_names[[1]]
    noncase_ids <- list_data_surv_noncase_ids_interval_names[[2]]
    interval_names <-list_data_surv_noncase_ids_interval_names[[3]]
    ind_any_zeroeventperiod <- list_data_surv_noncase_ids_interval_names[[4]]
  }
  
  # covars$ever_hf_cm <- factor(1*(((as.numeric(covars$cov_ever_hf)-1)+ (as.numeric(covars$cov_ever_cardiomyopathy)-1)) > 0))
  # covars <- covars %>% dplyr::select(! c("cov_ever_hf", "cov_ever_cardiomyopathy"))
  
  covar_names <- names(covars)[ names(covars) != "NHS_NUMBER_DEID"]
  
  data_surv <- data_surv %>% left_join(covars)
  data_surv <- data_surv %>% mutate(SEX = factor(SEX))
  data_surv$cox_weights <- ifelse(data_surv$NHS_NUMBER_DEID %in% noncase_ids, 1/noncase_frac, 1)

  cat("... data_surv ... \n")
  print(data_surv)
  
  gc()
  
  # ------------------- FIXED COVARIATES POST_BACKWARDS-SELECTION --------------
  # for vaccine project these are arterial/venous specific
  # fixed_covars <- c("ANTICOAG_MEDS", "ANTIPLATLET_MEDS", "BP_LOWER_MEDS", "CATEGORISED_ETHNICITY", "EVER_ALL_STROKE",
  #                     "EVER_AMI", "EVER_DIAB_DIAG_OR_MEDS", "IMD", "LIPID_LOWER_MEDS", "smoking_status") 
  # # "ANTIPLATELET_MEDS"
  # 
  # umbrella_venous <- c("DVT_summ_event","PE", 
  #                      "portal_vein_thrombosis",  "ICVT_summ_event", "other_DVT", "Venous_event")
  # 
  # 
  # if (event %in% umbrella_venous){
  #   fixed_covars <- c("CATEGORISED_ETHNICITY", "IMD", "ANTICOAG_MEDS", "COCP_MEDS", "HRT_MEDS", "EVER_PE_VT",  "COVID_infection")
  #   cat("...... VENOUS EVENT FIXED_COVARS! ...... \n")
  # }
  fixed_covars <- c() # hack for skipping backward-selection
  
  if (sex_as_interaction){
    # ---------------------  *sex as interaction term  -------------------------
    levels(data_surv$IMD) <- c("Deciles_1_2", "Deciles_3_4", "Deciles_5_6", "Deciles_7_8", "Deciles_9_10", "Deciles_9_10")
    fit <- coxfit_bkwdselection(data_surv, "all", interval_names, fixed_covars,event, agegp, sex_as_interaction, covar_names)
  } else {
    # ------------------------------  + sex  -----------------------------------
    fit_sexall <- coxfit_bkwdselection(data_surv, "all", interval_names, fixed_covars, event, agegp, sex_as_interaction, covar_names)
    fit_sex1 <- coxfit_bkwdselection(data_surv %>% filter(SEX==1), "1", interval_names, fixed_covars, event, agegp, sex_as_interaction, covar_names)
    fit_sex2 <- coxfit_bkwdselection(data_surv %>% filter(SEX==2), "2", interval_names, fixed_covars, event, agegp, sex_as_interaction, covar_names)
    fit <- rbindlist(list(fit_sexall, fit_sex1, fit_sex2))
  }


  fit$event <- event
  fit$agegp <- agegp
  
  cat("... fit ... \n")
  print(fit)
  
  
  write.csv(fit, paste0("tbl_hr_" , expo, "_", event, "_", agegp, ".csv"), row.names = F)

  
  gc()
  
}



mk_factor_orderlevels <- function(df, colname)
{
  df <- df %>% dplyr::mutate(
    !!sym(colname) := factor(!!sym(colname), levels = str_sort(unique(df[[colname]]), numeric = TRUE)))
  return(df)
}



one_hot_encode <- function(interacting_feat, data_surv, interval_names_withpre){
  cat("...... one_hot_encode ...... \n")
  data_surv <- as.data.frame(data_surv)
  data_surv$week <- apply(data_surv[unlist(interval_names_withpre)], 1, function(x) names( x[x==1]) )
  data_surv$week <- relevel(as.factor( data_surv$week) , ref="week_pre")

  data_surv$tmp <- as.factor(paste(data_surv$week, data_surv[[interacting_feat]], sep="_"))
  df_tmp <- as.data.frame(model.matrix( ~ 0 + tmp, data = data_surv))
  names(df_tmp) <- substring(names(df_tmp), 4)

  for (colname in names(df_tmp)){
    print(colname)
    df_tmp <- mk_factor_orderlevels(df_tmp, colname)
  }

  data_surv <- cbind(data_surv, df_tmp)

  str(data_surv)
  return(data_surv)
}
