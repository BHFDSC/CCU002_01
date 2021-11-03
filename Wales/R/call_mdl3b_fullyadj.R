# ******************************************************************************
# SAIL project: WMCC - Wales Multi-morbidity Cardiovascular COVID-19 UK
#               CCU002-01: SARS-CoV-2 infection and risk of venous thromboembolism and arterial thrombotic events
# About:        Prep outcome and analysis specific dataset for fully adjusted model
# ******************************************************************************
source(file.path(scripts_dir,"fit_model_reducedcovariates.R"))

#-------------------------------------------------------------------------------
get_vacc_res <- function(sex_as_interaction, event, agegp, cohort_vac, covars){

  # read in event dates for outcome-of-interest
  outcomes <- fread(master_df_fpath,
                    select=c("ALF_E",
                             paste0("OUT_", event)
                    ))
  # wrangle columns for naming convention
  setnames(outcomes,
           old = c(paste0("OUT_", event)),
           new = c("record_date"))
  outcomes$name <- event

  # join core data with outcomes
  survival_data <- cohort_vac %>% left_join(outcomes)

  # detect if a column is of date type, if so impose study start/end dates
  schema <- sapply(survival_data, is.Date)
  for (colname in names(schema)[schema==TRUE]){
    print(colname)
    survival_data <- set_dates_outofrange_na(survival_data, colname)
  }

  names(survival_data)[names(survival_data) == 'EXPO_DATE'] <- 'expo_date'

  cat( "...... survival_data...... \n")
  print(head(survival_data))

  res_vacc <- fit_model_reducedcovariates(sex_as_interaction, covars, agegp, event, survival_data)

  return(res_vacc)
}
