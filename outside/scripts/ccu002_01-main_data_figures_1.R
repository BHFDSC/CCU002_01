rm(list = ls())

files <- c("AgeSexRegion_HF_angina_stroke_SAH_HS_stroke_TIA_AllAge_AllSex.csv", 
           "Extensive_HF_angina_stroke_SAH_HS_stroke_TIA_AllAge_AllSex.csv",
           "Extensive_angina_stroke_SAH_HS_stroke_TIA_AllAge_AllSex_Pheno.csv",
           "AgeSexRegion_AllOutcomes_AllAge_AllSex.csv",
           "Extensive_AMI_stroke_isch_PE_DVT_event_AllAge_AllSex.csv",
           "Extensive_AMI_stroke_isch_PE_DVT_AllAge_AllSex_Phenotype.csv",
           "Extensive_HF_AllAge_AllSex_Phenotype.csv")

stratum <- c("Age/sex/region adjustment",
             "Extensive adjustment",
             "Hospitalised/Non-hospitalised COVID-19",
             "Age/sex/region adjustment",
             "Extensive adjustment",
             "Hospitalised/Non-hospitalised COVID-19",
             "Hospitalised/Non-hospitalised COVID-19")


# Make a single dataframe containing all estimates -----------------------------

df <- NULL

for (i in 1:length(files)) {
  
  tmp <- data.table::fread(paste0("raw/estimates/",files[i]),
                           data.table = FALSE)
  
  tmp <- tmp[grepl("week",tmp$term),]
  
  tmp$adjustment <- gsub("_.*","",files[i])
  
  tmp$stratum <- stratum[i]
  
  tmp$source <- files[i]
  
  df <- plyr::rbind.fill(df, tmp)
  
}

# Tidy variables ---------------------------------------------------------------

df$stratification <- ""

df$stratification <- ifelse(df$stratum %in% c("Age/sex/region adjustment","Extensive adjustment"),
                            "Overall",df$stratification)

df$stratification <- ifelse(df$stratum=="Hospitalised/Non-hospitalised COVID-19",
                            "Hospitalised/Non-hospitalised COVID-19",df$stratification)

df$stratum <- ifelse(df$stratum=="Hospitalised/Non-hospitalised COVID-19" & df$covidpheno=="non_hospitalised",
                     "Non-hospitalised COVID-19",df$stratum)

df$stratum <- ifelse(df$stratum=="Hospitalised/Non-hospitalised COVID-19" & df$covidpheno=="hospitalised",
                     "Hospitalised COVID-19",df$stratum)

# Tidy variables ---------------------------------------------------------------

df[,c("V1","covidpheno")] <- NULL

# Identify results that need to be combined by age group -----------------------

needs_meta <- unique(df[,c("event","agegp","sex","stratum")])
needs_meta <- needs_meta[!(needs_meta$agegp=="all"),]
needs_meta <- needs_meta[!grepl("Age group",needs_meta$stratum),]
needs_meta$agegp <- NULL
needs_meta <- unique(needs_meta)

# Perform meta-analysis for each event/stratum combination ---------------------

for (i in 1:nrow(needs_meta)) {
  
  tmp <- df[df$event==needs_meta$event[i] &
              df$sex==needs_meta$sex[i] &
              df$stratum==needs_meta$stratum[i],]
  
  meta <- unique(tmp[,c("event","sex","stratum","term","adjustment","stratification")])
  
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

# Save final estimates ---------------------------------------------------------

data.table::fwrite(df,"data/ccu002_01_main_data_figures_1.csv")