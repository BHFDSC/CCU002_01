rm(list = ls())

# Specify files containing estimates -------------------------------------------

files <- c("AgeSexRegion_Arterial_AllAge_AllSex.csv", # Overall: age/sex/region adjustment
           "Extensive_Arterial_AllAge_AllSex.csv", # Overall: extensive adjustment
           "Extensive_Arterial_AllAge_AllSex.csv", # Age group:
           "Extensive_Arterial_AllAge_Sex1.csv", # Sex: Male
           "Extensive_Arterial_AllAge_Sex2.csv", # Sex: Female 
           "Extensive_Arterial_NonHospitalised_AllAge_AllSex.csv", # Non-hospitalised COVID-19
           "Extensive_Arterial_Hospitalised_AllAge_AllSex_For_Meta_Analysis_Only.csv", # Hospitalised COVID-19
           "Extensive_ArterialVenous_AH1_AllAge_AllSex.csv", # Prior history of event
           "Extensive_ArterialVenous_AH0_AllAge_AllSex.csv", # No prior history of event
           "Extensive_Ethnic_Black_Allage_AllSex.csv", # Ethnicity: Black or Black British
           "Extensive_Ethnic_Mixed_AllAge_AllSex.csv", # Ethnicity: Mixed
           "Extensive_Ethnic_Others_AllAge_AllSex.csv", # Ethnicity: Other Ethnic Groups
           "Extensive_Ethnic_White_Allage_AllSex.csv", # Ethnicity: White
           "Extensive_Ethnic_Asian_Allage_AllSex.csv", # Ethnicity: Asian or Asian British
           "AgeSexRegion_Venous_AllAge_AllSex.csv",
           "Extensive_Venous_AllAge_AllSex.csv",
           "Extensive_Venous_AllAge_AllSex.csv", 
           "Extensive_Venous_AllAge_Sex1.csv",
           "Extensive_Venous_AllAge_Sex2.csv",
           "Extensive_Venous_NonHospitalised_AllAge_AllSex.csv",
           "Extensive_Venous_Hospitalised_AllAge_AllSex_For_Meta_Analysis_Only.csv",
           "Extensive_ArterialVenous_VH1_AllAge_AllSex.csv",
           "Extensive_ArterialVenous_VH0_AllAge_AllSex.csv")

# Specify stratum corresponding to files ---------------------------------------

stratum <- c("Age/sex/region adjustment",
             "Extensive adjustment",
             "Age group:",
             "Sex: Male",
             "Sex: Female",
             "Non-hospitalised COVID-19",
             "Hospitalised COVID-19",
             "Prior history of event",
             "No prior history of event",
             "Ethnicity: Black or Black British",
             "Ethnicity: Mixed",
             "Ethnicity: Other Ethnic Groups",
             "Ethnicity: White",
             "Ethnicity: Asian or Asian British",
             "Age/sex/region adjustment",
             "Extensive adjustment",
             "Age group:",
             "Sex: Male",
             "Sex: Female",
             "Non-hospitalised COVID-19",
             "Hospitalised COVID-19",
             "Prior history of event",
             "No prior history of event")

# Make a single dataframe containing all estimates -----------------------------

df <- NULL

for (i in 1:length(files)) {
  
  tmp <- data.table::fread(paste0("raw/estimates/",files[i]),
                           data.table = FALSE)
  
  tmp <- tmp[grepl("week",tmp$term),]
  
  tmp$adjustment <- gsub("_.*","",files[i])
  
  tmp$stratum <- stratum[i]
  
  tmp$source <- files[i]
  
  tmp$nation <- "England"
  
  df <- plyr::rbind.fill(df, tmp)
  
}

df$V1 <- NULL
df$agegp <- ifelse(df$agegp=="all_ages","all",df$agegp)

# Prepare Welsh data -----------------------------------------------------------

files <- c("AgeSexRegion_AllAges_Wales_211014.xlsx",
           "Extensive_AllAges_Wales_211014.xlsx",
           "Extensive_AllAges_Wales_211014.xlsx",
           "Extensive_AllAges_Wales_211014.xlsx")

stratum <- c("Age/sex/region adjustment",
             "Extensive adjustment",
             "Sex: Male",
             "Sex: Female")

for (i in 1:length(files)) {
  
  tmp <- readxl::read_excel(paste0("raw/estimates/",files[i]))
  
  tmp$agegp <- tmp$`age group`
  tmp$`age group` <- NULL
  tmp$`...1` <- NULL
  
  tmp <- tmp[grepl("week",tmp$term),]
  
  tmp$adjustment <- gsub("_.*","",files[i])
  
  tmp$stratum <- stratum[i]
  
  tmp$source <- files[i]
  
  if (stratum[i] %in% c("Sex: Male","Sex: Female")) {
    tmp <- tmp[tmp$sex!="all",]
  } else {
    tmp <- tmp[tmp$sex=="all",]
  }
  
  tmp$nation <- "Wales"
  
  df <- plyr::rbind.fill(df, tmp)
  
}

# Tidy variables ---------------------------------------------------------------

df$stratum <- ifelse(df$stratum=="Age group:",paste(df$stratum,df$agegp),df$stratum)

df$stratification <- gsub(":.*","",df$stratum)

df$stratification <- ifelse(df$stratum %in% c("Age/sex/region adjustment","Extensive adjustment"),
                            "Overall",df$stratification)

df$stratification <- ifelse(df$stratum %in% c("Non-hospitalised COVID-19","Hospitalised COVID-19"),
                            "Hospitalised/Non-hospitalised COVID-19",df$stratification)

df$stratification <- ifelse(df$stratum %in% c("Prior history of event","No prior history of event"),
                            "Prior history of event",df$stratification)

df <- df[!(df$event=="Arterial_event" & df$source=="Extensive_ArterialVenous_VH0_AllAge_AllSex.csv"),]
df <- df[!(df$event=="Arterial_event" & df$source=="Extensive_ArterialVenous_VH1_AllAge_AllSex.csv"),]
df <- df[!(df$event=="Venous_event" & df$source=="Extensive_ArterialVenous_AH0_AllAge_AllSex.csv"),]
df <- df[!(df$event=="Venous_event" & df$source=="Extensive_ArterialVenous_AH1_AllAge_AllSex.csv"),]

# Identify results that need to be combined by age group -----------------------

needs_meta <- unique(df[,c("event","agegp","sex","stratum","nation")])
needs_meta <- needs_meta[!(needs_meta$agegp=="all"),]
needs_meta <- needs_meta[!grepl("Age group",needs_meta$stratum),]
needs_meta$agegp <- NULL
needs_meta <- unique(needs_meta)

# Perform meta-analysis --------------------------------------------------------

for (i in 1:nrow(needs_meta)) {
  
  tmp <- df[df$event==needs_meta$event[i] &
              df$sex==needs_meta$sex[i] &
              df$stratum==needs_meta$stratum[i] &
              df$nation==needs_meta$nation[i],]
  
  meta <- unique(tmp[,c("event","sex","stratum","nation","term","adjustment","stratification")])
  
  meta$agegp <- "all"
  meta$source <- "meta-analysis (age groups)"
  
  meta$estimate <- NA
  meta$conf.low <- NA
  meta$conf.high <- NA
  meta$p.value <- NA
  meta$std.error <- NA
  meta$robust.se <- NA
  meta$statistic <- NA
  
  for (j in unique(meta$term)) {
    tmp2 <- tmp[tmp$term==j,]
    tmp_meta <- meta::metagen(log(tmp2$estimate),tmp2$std.error, sm = "HR")
    meta[meta$term==j,]$estimate <- exp(tmp_meta$TE.fixed)
    meta[meta$term==j,]$conf.low <- exp(tmp_meta$lower.fixed)
    meta[meta$term==j,]$conf.high <- exp(tmp_meta$upper.fixed)
    meta[meta$term==j,]$p.value <- tmp_meta$pval.fixed
    meta[meta$term==j,]$std.error <- tmp_meta$seTE.fixed
  }
  
  df <- plyr::rbind.fill(df, meta)
  
}

# Tidy variables ---------------------------------------------------------------

df$covidpheno <- NULL

# Identify results that need to be combined by nation --------------------------

needs_meta <- unique(df[df$nation=="Wales",c("event","agegp","sex","stratum")])

# Perform meta-analysis --------------------------------------------------------

for (i in 1:nrow(needs_meta)) {
  
  tmp <- df[df$event==needs_meta$event[i] &
              df$agegp==needs_meta$agegp[i] &
              df$sex==needs_meta$sex[i] &
              df$stratum==needs_meta$stratum[i],]
  
  meta <- unique(tmp[,c("event","agegp","sex","stratum","term","adjustment","stratification")])
  
  meta$nation <- "all"
  meta$source <- "meta-analysis (nations)"
  
  meta$estimate <- NA
  meta$conf.low <- NA
  meta$conf.high <- NA
  meta$p.value <- NA
  meta$std.error <- NA
  meta$robust.se <- NA
  meta$statistic <- NA
  
  for (j in unique(meta$term)) {
    tmp2 <- tmp[tmp$term==j,]
    tmp_meta <- meta::metagen(log(tmp2$estimate),tmp2$std.error, sm = "HR")
    meta[meta$term==j,]$estimate <- exp(tmp_meta$TE.fixed)
    meta[meta$term==j,]$conf.low <- exp(tmp_meta$lower.fixed)
    meta[meta$term==j,]$conf.high <- exp(tmp_meta$upper.fixed)
    meta[meta$term==j,]$p.value <- tmp_meta$pval.fixed
    meta[meta$term==j,]$std.error <- tmp_meta$seTE.fixed
  }
  
  df <- plyr::rbind.fill(df, meta)
  
}


# Save final estimates ---------------------------------------------------------

data.table::fwrite(df,"data/ccu002_01_main_data_figures_1_2.csv")