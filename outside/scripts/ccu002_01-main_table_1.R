# Read in Databricks output

df <- data.table::fread("raw/table_1.csv", data.table = FALSE)

# Prepare denominators ---------------------------------------------------------

denom <- unique(df[,c("category","N")])
colnames(denom) <- c("category","value")
denom$variable1 <- "N"
denom$variable2 <- ""
denom <- denom[,c("category","variable1","variable2","value")]

# Prepare Table 1 covariates ---------------------------------------------------

df <- tidyr::pivot_longer(df, cols = setdiff(colnames(df),c("category","N")))
df$variable1 <- stringr::str_split_fixed(df$name, "_", 2)[,1]
df$variable2 <- stringr::str_split_fixed(df$name, "_", 2)[,2]
df <- df[,c("category","variable1","variable2","value")]

# Combine covariates and denominators ------------------------------------------

df <- rbind(denom,df)

# Pivot to wide table ----------------------------------------------------------

df <- tidyr::pivot_wider(df, id_cols = c("variable1","variable2"), names_from = "category", values_from = "value")

# Remove medical history -------------------------------------------------------

df <- df[!(df$variable1=="MedicalHistory" & !(df$variable2 %in% c("ArterialEvent","VenousEvent"))),]
df <- df[!(df$variable1=="Medication"),]

# Tidy variables ---------------------------------------------------------------

df$variable1 <- ifelse(df$variable1=="SmokingStatus", "Smoking status", df$variable1)
df$variable1 <- ifelse(df$variable1=="MedicalHistory", "Medical history", df$variable1)
df$variable1 <- ifelse(df$variable1=="Medication", "Medications", df$variable1)
df$variable1 <- ifelse(df$variable1=="NumberOfDiagnoses", "Number of diagnoses", df$variable1)
df$variable1 <- ifelse(df$variable1=="NumberofDiagnoses", "Number of diagnoses", df$variable1)
df$variable1 <- ifelse(df$variable1=="Unique", "Number of medications", df$variable1)

df$variable2 <- gsub("_"," - ",df$variable2)
df$variable2 <- ifelse(df$variable2=="Asian", "Asian or Asian British", df$variable2)
df$variable2 <- ifelse(df$variable2=="Black", "Black or Black British", df$variable2)
df$variable2 <- ifelse(df$variable2=="Other", "Other Ethnic Groups", df$variable2)
df$variable2 <- ifelse(df$variable2=="90plus", "90+", df$variable2)
df$variable2 <- ifelse(df$variable2=="6plus", "6+", df$variable2)
df$variable2 <- ifelse(df$variable2=="ArterialEvent", "Arterial event(s)", df$variable2)
df$variable2 <- ifelse(df$variable2=="VenousEvent", "Venous event(s)", df$variable2)
df$variable2 <- ifelse(df$variable2=="bnf - chaps - 0", "0", df$variable2)
df$variable2 <- ifelse(df$variable2=="bnf - chaps - 1 - 5", "1 - 5", df$variable2)
df$variable2 <- ifelse(df$variable2=="bnf - chaps - 6plus", "6+", df$variable2)
# df$variable2 <- ifelse(df$variable2=="Lipid", "Lipid lowering", df$variable2)
# df$variable2 <- ifelse(df$variable2=="COCP - OralContraceptive", "Oral contraceptive", df$variable2)
df$variable2 <- ifelse(df$variable2=="North - West","North West", df$variable2)
df$variable2 <- ifelse(df$variable2=="South - East","South East", df$variable2)
df$variable2 <- ifelse(df$variable2=="EastOfEngland","East of England", df$variable2)
df$variable2 <- ifelse(df$variable2=="SouthWest","South West", df$variable2)
df$variable2 <- ifelse(df$variable2=="Yorkshire - and - TheHumber","Yorkshire and the Humber", df$variable2)
df$variable2 <- ifelse(df$variable2=="EastMidlands","East Midlands", df$variable2)
df$variable2 <- ifelse(df$variable2=="WestMidlands","West Midlands", df$variable2)
df$variable2 <- ifelse(df$variable2=="NorthEast","North East", df$variable2)

# Order variables --------------------------------------------------------------

df <- df[,c("variable1","variable2",
            "whole_all","hospitalised_all","non_hospitalised_all")]


# Add risk per 100000 for all events -------------------------------------------

# df$no_covid_all <- ifelse(as.numeric(sprintf("%.0f",100000*(df$no_covid_all/df$whole_all)))<100,
#                         paste0(df$no_covid_all," (",sprintf("%.1f",100000*(df$no_covid_all/df$whole_all)),")"),
#                         paste0(df$no_covid_all," (",sprintf("%.0f",100000*(df$no_covid_all/df$whole_all)),")"))

df$non_hospitalised_all <- ifelse(as.numeric(sprintf("%.0f",100000*(df$non_hospitalised_all/df$whole_all)))<100,
                          paste0(df$non_hospitalised_all," (",sprintf("%.1f",100000*(df$non_hospitalised_all/df$whole_all)),")"),
                          paste0(df$non_hospitalised_all," (",sprintf("%.0f",100000*(df$non_hospitalised_all/df$whole_all)),")"))

df$hospitalised_all <- ifelse(as.numeric(sprintf("%.0f",100000*(df$hospitalised_all/df$whole_all)))<100,
                                  paste0(df$hospitalised_all," (",sprintf("%.1f",100000*(df$hospitalised_all/df$whole_all)),")"),
                                  paste0(df$hospitalised_all," (",sprintf("%.0f",100000*(df$hospitalised_all/df$whole_all)),")"))

# Save table1 ------------------------------------------------------------------

data.table::fwrite(df,"output/ccu002_01_main_table_1.csv")