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

df[,c("V1","covidpheno")] <- NULL
df$agegp <- ifelse(df$agegp=="all_ages","all",df$agegp)

# Prepare Welsh data -----------------------------------------------------------

files <- c("AgeSexRegion_AllAges_Wales_211014.xlsx",
           "Extensive_AllAges_Wales_211014.xlsx",
           "Extensive_AllAges_Wales_211014.xlsx",
           "Extensive_AllAges_Wales_211014.xlsx",
           "Extensive_Arterial_AgeGroups_Wales_211015.xlsx",
           "Extensive_Venous_AgeGroups_Wales_211015.xlsx")#,
           #"COVIDphen_FullyAdjusted_AllAges.xlsx")

stratum <- c("Age/sex/region adjustment",
             "Extensive adjustment",
             "Sex: Male",
             "Sex: Female",
             "Age group:",
             "Age group:")#,
             #"Hospitalised COVID-19/Non-hospitalised COVID-19")

for (i in 1:length(files)) {
  
  tmp <- readxl::read_excel(paste0("raw/estimates/",files[i]))
  
  if ("age group" %in% colnames(tmp)) {
    tmp$agegp <- tmp$`age group`
    tmp$`age group` <- NULL
  }
  
  tmp$`...1` <- NULL
  
  tmp <- tmp[grepl("week",tmp$term),]
  
  tmp$adjustment <- gsub("_.*","",files[i])
  
  tmp$stratum <- stratum[i]
  
  tmp$source <- files[i]
  
  if(stratum[i] %in% c("Hospitalised COVID-19/Non-hospitalised COVID-19")) {
    tmp$adjustment <- "Extensive"
    tmp$stratum <- ifelse(tmp$`COVIOD phenotype`=="hospitalised","Hospitalised COVID-19",tmp$stratum)
    tmp$stratum <- ifelse(tmp$`COVIOD phenotype`=="non-hospitalised","Non-hospitalised COVID-19",tmp$stratum)
    tmp$`COVIOD phenotype` <- NULL
  }
  
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
df <- df[!(df$stratum=="Sex: Male" & df$sex==2),]
df <- df[!(df$stratum=="Sex: Female" & df$sex==1),]

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

needs_meta <- unique(df[df$nation=="Wales",c("event","agegp","sex","stratum","term")])
needs_meta <- needs_meta[needs_meta$term %in% c("week1","week2","week3_4","week5_8","week9_12","week13_26","week27_49"),]

# Perform meta-analysis --------------------------------------------------------

for (i in 1:nrow(needs_meta)) {
  
  tmp <- df[df$event==needs_meta$event[i] &
              df$agegp==needs_meta$agegp[i] &
              df$sex==needs_meta$sex[i] &
              df$stratum==needs_meta$stratum[i] &
              df$term==needs_meta$term[i],]
  
  meta <- unique(tmp[,c("event","agegp","sex","stratum","term","adjustment","stratification")])
  
  meta$robust.se <- NA
  meta$statistic <- NA
  meta$nation <- "all"
  meta$source <- "meta-analysis (nations)"

  tmp_meta <- meta::metagen(log(tmp$estimate),tmp$std.error, sm = "HR")
  meta$estimate <- exp(tmp_meta$TE.fixed)
  meta$conf.low <- exp(tmp_meta$lower.fixed)
  meta$conf.high <- exp(tmp_meta$upper.fixed)
  meta$p.value <- tmp_meta$pval.fixed
  meta$std.error <- tmp_meta$seTE.fixed
  
  df <- plyr::rbind.fill(df, meta)
  
}


# Save final estimates ---------------------------------------------------------

data.table::fwrite(df,"data/ccu002_01_main_data_figures_2_3.csv")


# 

tmp <- df 
tmp <- tmp[(tmp$sex=="all" & tmp$agegp=="all") | (tmp$stratification=="Age group") | (tmp$stratification=="Sex" & tmp$agegp=="all"),]
tmp <- tmp[tmp$term %in% c("week1","week2","week3_4","week5_8","week9_12","week13_26","week27_49"),]
tmp$est <- paste0(ifelse(tmp$estimate>=10,sprintf("%.1f",tmp$estimate),sprintf("%.2f",tmp$estimate)),
                  " (",ifelse(tmp$conf.low>=10,sprintf("%.1f",tmp$conf.low),sprintf("%.2f",tmp$conf.low)),
                  "-",ifelse(tmp$conf.high>=10,sprintf("%.1f",tmp$conf.high),sprintf("%.2f",tmp$conf.high)),")")
tmp <- tmp[,c("event","stratum","nation","term","est")]
tmp <- tidyr::pivot_wider(tmp, names_from = "nation", values_from = c("est"))
tmp$event <- ifelse(tmp$event=="Arterial_event","First arterial thrombosis",tmp$event)
tmp$event <- ifelse(tmp$event=="Venous_event","First venous thromboembolism",tmp$event)
data.table::fwrite(tmp[tmp$event=="First arterial thrombosis",],"output/CompareNations_arterial.csv")
data.table::fwrite(tmp[tmp$event=="First venous thromboembolism",],"output/CompareNations_venous.csv")

