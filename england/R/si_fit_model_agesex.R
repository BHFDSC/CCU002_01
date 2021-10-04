#-------------------------------------------------------------------------------
# Samantha's prelim code adapted from Jenny's CCU002-R-automatedmodelling.R, loop from Jenny and Venexia
# survSplitfrom Tom Bolton 
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
# WRANGLE SURVIVAL DATE FROM DATABRICKS
#-------------------------------------------------------------------------------
# rm(list=setdiff(ls(), c("con", "survival_data")))
source("/mnt/efs/hyi20/dars_nic_391419_j3w9t_collab/CCU002_vac/scripts/si_fit_get_data_surv.R")


fit_model_justweeks_age <- function(covars, vac_str, agebreaks, agelabels, agegp, event, survival_data, cuts_weeks_since_expo, cohort_start_date="2020-12-08", cohort_end_date="2021-03-18", expo="VACCINE", noncase_frac=0.1){
  
  #===============================================================================
  # GET data_surv -- with age, sex, region
  #-------------------------------------------------------------------------------
  list_data_surv_noncase_ids_interval_names <- fit_get_data_surv(covars, vac_str, agebreaks, agelabels, agegp, event, survival_data, cuts_weeks_since_expo, cohort_start_date="2020-12-08", cohort_end_date="2021-03-18", expo="VACCINE", noncase_frac=0.1)
  data_surv <- list_data_surv_noncase_ids_interval_names[[1]]
  noncase_ids <- list_data_surv_noncase_ids_interval_names[[2]]
  interval_names <-list_data_surv_noncase_ids_interval_names[[3]]
  
  #===============================================================================
  # COXPH
  #-------------------------------------------------------------------------------
  data_surv$cox_weights <- ifelse(data_surv$NHS_NUMBER_DEID %in% noncase_ids, 1/noncase_frac, 1)
  print("data_surv")
  print(head(data_surv))

  
  coxfit <- function(data_surv, sex, interval_names){
    fml <- paste0(
      "Surv(tstart, tstop, event) ~ age + age_sq + ", 
      paste(interval_names, collapse="+"),
      "+ cluster(NHS_NUMBER_DEID) + strata(region_name)")
    if (sex == "all"){
      fml<- paste(fml, "SEX", sep ="+")
    }
    print(sex)
    print(fml)
    
    fit <- coxph(
      as.formula(fml), 
      data= data_surv, weights=data_surv$cox_weights
    )
    
    fit_tidy <-tidy(fit, exponentiate = TRUE, conf.int=TRUE)
    fit_tidy$sex<-c(sex)
    print(fit_tidy)
    return(fit_tidy)
  }


  fit_sexall <- coxfit(data_surv, "all", interval_names)
  fit_sex1 <- coxfit(data_surv %>% filter(SEX==1), "1", interval_names)
  fit_sex2 <- coxfit(data_surv %>% filter(SEX==2), "2", interval_names)
  
  fit <- rbindlist(list(fit_sexall, fit_sex1, fit_sex2), fill=TRUE)
  

  fit$event <- event
  fit$agegp <- agegp
  fit$vac <- vac_str
  
  write.csv(fit, paste0("tbl_hr_" , expo, "_", event, "_", agegp, "_", vac_str, ".csv"), row.names = T)
  
  print(paste0("finished......", event, "......!!! :D"))
  
  
  #===============================================================================
  # PLOT HR
  #-------------------------------------------------------------------------------
  # fit_sexall_tidy$term <- factor(fit_sexall_tidy$term, levels=unique(fit_sexall_tidy$term ))
  # 
  # p <- ggplot(fit_sexall_tidy, aes(x=term, y=estimate, group = 1)) + 
  #   geom_line() +
  #   geom_errorbar(aes(ymin=conf.low, ymax=conf.high), width=.2, col="grey",
  #                 position=position_dodge(0.05))+
  #   geom_point()+
  #   theme(axis.text.x = element_text (angle = -50)) +
  #   labs(x = paste0("Weeks since ", expo), y = paste0("Hazard ratio for ", event, " (crude)"), 
  #        title = paste("Hazard ratios for", event, "with time since", expo, "for", agegp, "(crude)"), sep=" ") +
  #   geom_hline(yintercept=1, lwd=0.5, col="red4", linetype = "dashed")
  # 
  # p
  # 
  # 
  # #Save this output event
  # ggsave(p, file=paste0("plt_hr_", expo, "_", event, "_", agegp, ".png"), scale=1)
  # 
  # 
  # print(paste0("HR plot done: plt_hr_", expo, "_", event, "_", agegp, ".png"))

  gc()
  # return(output)
  
  
  }



