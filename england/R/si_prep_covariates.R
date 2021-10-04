##rk
## =============================================================================
## WRANGLES & FORMATS DATA TABLE -- sets data types, deals with missing entries, 
## defines reference levels
##
## Author: Samantha Ip
## =============================================================================
library(stringr)

#----------------------- REPLACE " " with "_" for glht's linfct-----------------
covars$cov_ethncity <- gsub(" ", "_", covars$cov_ethncity)
cohort_vac$region_name <- gsub(" ", "_", cohort_vac$region_name)

print(unique(covars$cov_ethncity))

#---------------------------- REPLACE "" with "missing"------------------------------
covars$cov_deprivation[covars$cov_deprivation == ""] <- "Missing"

#-------------------------------- FACTOR ---------------------------------------
factor_covars <- names(covars %>% dplyr::select(! c("NHS_NUMBER_DEID", "cov_n_disorder", "cov_unique_bnf_chaps")))
#factor_covars <- names(covars)[names(covars) != "NHS_NUMBER_DEID"]

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

cohort_vac <- mk_factor_orderlevels(cohort_vac, "region_name")

##---------------------------- REPLACE NA with "0"------------------------------
# na_to_0_covars <- names(covars %>% select_if(~ nlevels(.) == 2) )
# covars <- as.data.frame(covars)
# covars[na_to_0_covars][is.na(covars[na_to_0_covars])] <- 0



##--------------------------- REPLACE "missing" with "0"------------------------
# mk_missing_to_0 <- function(covars, colname)
# {
#   covars <- covars %>% mutate(
#     !!sym(colname) := recode(!!sym(colname), "missing"="0"))
#   return(covars)
# }
# 
# for (colname in missing_to_0_covars){
#   print(colname)
#   covars <- mk_missing_to_0(covars, colname)
# }
# 
# 
# covars$cov_smoking_status_ <- recode(covars$cov_smoking_status_, "0"="missing")

##------------------------------ any NAs left? ---------------------------------
any(is.na(covars))
colnames(covars)[colSums(is.na(covars)) > 0]

##------------------------- ADD MISSING AS EXTRA LEVEL -------------------------
# # Get levels and add "missing' 
# levels <- levels(covars$DECI_IMD)
# levels[length(levels) + 1] <- "missing"
# covars$DECI_IMD <- factor(covars$DECI_IMD, levels = levels)
# covars$DECI_IMD[is.na(covars$DECI_IMD)] <- "missing"
# levels(covars$DECI_IMD)

##---------------------------- RELEVEL -----------------------------------------
# The few factors that have specific  non-alphabetical reference levels
covars$cov_ethncity <- relevel(covars$cov_ethncity, ref = "White")
covars$cov_smoking_status <- relevel(covars$cov_smoking_status, ref = "Missing")
cohort_vac$region_name <- relevel(factor(cohort_vac$region_name), ref = "London")


##------------------------------- NUMERIC --------------------------------------
# # The few covariates that should be numeric
numeric_covars <- c("cov_unique_bnf_chaps", "cov_n_disorder")
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
