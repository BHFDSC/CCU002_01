# ******************************************************************************
# SAIL project: WMCC - Wales Multi-morbidity Cardiovascular COVID-19 UK
#               CCU002-01: SARS-CoV-2 infection and risk of venous thromboembolism and arterial thrombotic events
# About:        Format covariates -- sets data types, deals with missing entries, defines reference levels
# ******************************************************************************
library(stringr)

#----------------------- REPLACE " " with "_" for glht's linfct-----------------
covars$COV_ETHNICITY <- gsub(" ", "_", covars$COV_ETHNICITY)

covars$COV_ETHNICITY[is.na(covars$COV_ETHNICITY)] <- "Missing"
print(unique(covars$COV_ETHNICITY))

#---------------------------- REPLACE "" with "missing"------------------------------
covars$COV_DEPRIVATION[is.na(covars$COV_DEPRIVATION)] <- "Missing"
print(unique(covars$COV_DEPRIVATION))

#-------------------------------- FACTOR ---------------------------------------
factor_covars <- names(covars %>% dplyr::select(! c("ALF_E", "COV_N_DISORDER", "COV_UNIQUE_BNF_CHAPS")))

mk_factor_orderlevels <- function(covars, colname)
  {
  covars <- covars %>% mutate(
    !!sym(colname) := factor(!!sym(colname), levels = str_sort(unique(covars[[colname]]), numeric = TRUE)))
  return(covars)
}

for (colname in factor_covars){
  print(colname)
  covars <- mk_factor_orderlevels(covars, colname)
}

#------------------------------ any NAs left? ---------------------------------
print("Check if any NA left...")
any(is.na(covars))
colnames(covars)[colSums(is.na(covars)) > 0]

#---------------------------- RELEVEL -----------------------------------------
# The few factors that have specific  non-alphabetical reference levels
covars$COV_ETHNICITY <- relevel(covars$COV_ETHNICITY, ref = "White")
covars$COV_SMOKING_STATUS <- relevel(covars$COV_SMOKING_STATUS, ref = "Missing")

#------------------------------- NUMERIC --------------------------------------
# The few covariates that should be numeric
numeric_covars <- c("COV_N_DISORDER", "COV_UNIQUE_BNF_CHAPS")
mk_numeric <- function(covars, colname)
{
   covars <- covars %>% mutate(
     !!sym(colname) := as.numeric(!!sym(colname)))
   return(covars)
}
for (colname in numeric_covars){
   print(colname)
   covars <- mk_numeric(covars, colname)
}

str(covars)
