rm(list = ls())

files <- c("AgeSexRegion_AllOutcomes_AllAge_AllSex.csv", # Overall: age/sex/region adjustment
           "Extensive_AMI_stroke_isch_PE_DVT_event_AllAge_AllSex.csv", # Overall: extensive adjustment
           "Extensive_AMI_stroke_isch_PE_DVT_event_AllAge_AllSex.csv", # Age group:
           "Extensive_AMI_stroke_isch_AllAge_AllSex_Phenotype.csv", # Non-hospitalised COVID-19 and Hospitalised COVID-19
           "Extensive_PE_AllAge_AllSex_Phenotype.csv") # Non-hospitalised COVID-19 and Hospitalised COVID-19
           

stratum <- c("Age/sex/region adjustment",
             "Extensive adjustment",
             "Age group:",
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
  
  for (eval in c("AMI","stroke_isch", "DVT_event","PE")){
    
    tmp2 <- tmp[grepl(eval,tmp$event),]
    df <- plyr::rbind.fill(df, tmp2)
    
  }
  

}

# Tidy variables ---------------------------------------------------------------

df$stratum <- ifelse(df$stratum=="Age group:",paste(df$stratum,df$agegp),df$stratum)

df$stratification <- gsub(":.*","",df$stratum)

df$stratification <- ifelse(df$stratum %in% c("Age/sex/region adjustment","Extensive adjustment"),
                            "Overall",df$stratification)

df$stratum <- ifelse(df$stratum=="Hospitalised/Non-hospitalised COVID-19" & df$covidpheno=="non_hospitalised",
                     "Non-hospitalised COVID-19",df$stratum)

df$stratum <- ifelse(df$stratum=="Hospitalised/Non-hospitalised COVID-19" & df$covidpheno=="hospitalised",
                     "Hospitalised COVID-19",df$stratum)

df[,c("V1","covidpheno")] <- NULL

# Identify events and stratum for meta-analysis --------------------------------

events <- unique(df$event)
stratum <- unique(df$stratum)[!grepl("Age group:",unique(df$stratum))]

# Perform meta-analysis for each event/stratum combination ---------------------

for (e in events) {
  for (s in stratum) {
    
    tmp <- df[df$stratum==s & df$event==e,]
    meta <- unique(tmp[,c("event","sex","term")])
    
    if (nrow(tmp)>nrow(meta)) {
      meta$agegp <- "all_ages"
      meta$estimate <- NA
      meta$conf.low <- NA
      meta$conf.high <- NA
      meta$p.value <- NA
      meta$std.error <- NA
      meta$robust.se <- NA
      meta$statistic <- NA
      meta$stratum <- s
      meta$adjustment <- tmp$adjustment[1]
      meta$stratification <- tmp$stratification[1]
      meta$source <- "meta-analysis"
      
      for (j in unique(meta$term)) {
        tmp2 <- tmp[tmp$term==j,]
        if (nrow(tmp2)>0) {
          tmp_meta <- meta::metagen(log(tmp2$estimate),tmp2$std.error, sm = "HR")
          meta[meta$term==j,]$estimate <- exp(tmp_meta$TE.fixed)
          meta[meta$term==j,]$conf.low <- exp(tmp_meta$lower.fixed)
          meta[meta$term==j,]$conf.high <- exp(tmp_meta$upper.fixed)
          meta[meta$term==j,]$p.value <- tmp_meta$pval.fixed
          meta[meta$term==j,]$std.error <- tmp_meta$seTE.fixed
        }
      }
      
      df <- plyr::rbind.fill(df, meta)
    }

  }
}

# Tidy variables ---------------------------------------------------------------

df$covidpheno <- NULL
df <- df[df$agegp=="all_ages" | df$stratification=="Age group",]
df$agegp <- ifelse(df$agegp=="all_ages","all",df$agegp)

# Save final estimates ---------------------------------------------------------

data.table::fwrite(df,"data/ccu002_01_suppl_figure_estimates.csv")