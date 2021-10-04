rm(list = ls())

# Define format for plot data --------------------------------------------------

df <- data.frame(stratification = character(),
                 stratum = character(),
                 event = character(),
                 term = character(),
                 estimate = numeric(),
                 conf.low = numeric(),
                 conf.high = numeric(),
                 stringsAsFactors = FALSE)

# Load and add ethnicity estimates to plot data --------------------------------

ethnicity_files <- list.files(path = "raw/estimates/", pattern = "FullAdjustment_AgeMeta_Ethnicity")

for (i in 1:length(ethnicity_files)) {
  
  tmp <- data.table::fread(paste0("raw/estimates/",ethnicity_files[i]), 
                           data.table = FALSE, 
                           select = c("event","term","estimate","conf.low","conf.high"))
  if (nrow(tmp)>0) {
    tmp$stratification <- "Ethnicity"
    tmp$stratum <- paste0("Ethnicity: ",gsub(".csv","",gsub("FullAdjustment_AgeMeta_Ethnicity","",ethnicity_files[i])))
    tmp <- tmp[,c("stratification","stratum","event","term","estimate","conf.low","conf.high")]
    df <- rbind(df, tmp)
  }
  
}

rm(list=setdiff(ls(), "df"))

# Load history estimates -------------------------------------------------------

pre_meta <- NULL

arterial0 <- data.table::fread("raw/estimates/FullAdjustment_AgeAll_Arterial0.csv", 
                               select = c("event","term","estimate","std.error"),
                               data.table = FALSE)
arterial0 <- arterial0[grepl("week",arterial0$term) & arterial0$event=="Arterial_event",]
arterial0$stratification <- "History"
arterial0$stratum <- "No history of event"
pre_meta <- rbind(pre_meta, arterial0)

arterial1 <- data.table::fread("raw/estimates/FullAdjustment_AgeAll_Arterial1.csv", 
                               select = c("event","term","estimate","std.error"),
                               data.table = FALSE)
arterial1 <- arterial1[grepl("week",arterial1$term) & arterial1$event=="Arterial_event",]
arterial1$stratification <- "History"
arterial1$stratum <- "History of event"
pre_meta <- rbind(pre_meta, arterial1)

venous0 <- data.table::fread("raw/estimates/FullAdjustment_AgeAll_venous0.csv", 
                             select = c("event","term","estimate","std.error"),
                             data.table = FALSE)
venous0 <- venous0[grepl("week",venous0$term) & venous0$event=="Venous_event",]
venous0$stratification <- "History"
venous0$stratum <- "No history of event"
pre_meta <- rbind(pre_meta, venous0)

venous1 <- data.table::fread("raw/estimates/FullAdjustment_AgeAll_venous1.csv",
                              select = c("event","term","estimate","std.error"),
                              data.table = FALSE)
venous1 <- venous1[grepl("week",venous1$term) & venous1$event=="Venous_event",]
venous1$stratification <- "History"
venous1$stratum <- "History of event"
pre_meta <- rbind(pre_meta, venous1)

# Load severity estimates ------------------------------------------------------

severity <- data.table::fread("raw/estimates/FullAdjustment_AgeAll_COVIDSeverity.csv", 
                              select = c("covidpheno","event","term","estimate","std.error"),
                              data.table = FALSE)
severity <- severity[grepl("week",severity$term) & grepl("_event",severity$event),]
severity <- dplyr::rename(severity, stratum = covidpheno)
severity$stratification <- "Severity"
severity$stratum <- ifelse(severity$stratum=="hospitalised","Hospitalised COVID-19",severity$stratum)
severity$stratum <- ifelse(severity$stratum=="non_hospitalised","Non-hospitalised COVID-19",severity$stratum)
pre_meta <- rbind(pre_meta, severity)

# Load overall extensively adjusted estimates ----------------------------------

overall_full <- data.table::fread("raw/estimates/FullAdjustment_AgeAll_NoStratification.csv",
                        select = c("event","term","estimate","std.error"),
                        data.table = FALSE)

overall_full <- overall_full[grepl("week",overall_full$term) & overall_full$event %in% c("Arterial_event","Venous_event"),]
overall_full$stratification <- "Adjustment"
overall_full$stratum <- "Adjustment: extensive"
pre_meta <- rbind(pre_meta, overall_full)

# Load overall age, sex, region adjusted estimates -----------------------------

overall_asr <- data.table::fread("raw/estimates/AgeSexRegionAdjustment_AgeAll_NoStratification.csv",
                                  select = c("event","term","estimate","std.error"),
                                  data.table = FALSE)
overall_asr <- overall_asr[grepl("week",overall_asr$term) & overall_asr$event %in% c("Arterial_event","Venous_event"),]
overall_asr$stratification <- "Adjustment"
overall_asr$stratum <- "Adjustment: age/sex/region"
pre_meta <- rbind(pre_meta, overall_asr)

# Meta-analysis overall estimates ----------------------------------------------

meta <- unique(pre_meta[,c("stratification","stratum","event","term")])
meta$estimate <- NA
meta$conf.low <- NA
meta$conf.high <- NA

for (i in unique(pre_meta$event)) {
  for (j in unique(pre_meta$term)) {
    for (k in unique(pre_meta$stratum)) {
      tmp <- pre_meta[pre_meta$event==i &pre_meta$term==j & pre_meta$stratum==k,]
      tmp <- na.omit(tmp)
      if (nrow(tmp)>0) {
        tmp_meta <- meta::metagen(tmp$estimate,tmp$std.error)
        meta[meta$event==i & meta$term==j & meta$stratum==k,]$estimate <- tmp_meta$TE.fixed
        meta[meta$event==i & meta$term==j & meta$stratum==k,]$conf.low <- tmp_meta$lower.fixed
        meta[meta$event==i & meta$term==j & meta$stratum==k,]$conf.high <- tmp_meta$upper.fixed
      }
    }
  }
}

df <- rbind(df,meta)
rm(list=setdiff(ls(), "df"))

# Load age stratified results --------------------------------------------------

age <- data.table::fread("raw/estimates/FullAdjustment_AgeAll_NoStratification.csv",
                         select = c("agegp","event","term","estimate","conf.low","conf.high"),
                         data.table = FALSE)
age <- age[grepl("week",age$term) & age$event %in% c("Arterial_event","Venous_event"),]
age <- dplyr::rename(age, stratum = agegp)
age$stratification <- "Age"
age$stratum <- paste0("Age: ",age$stratum)
df <- rbind(df,age)
rm(list=setdiff(ls(), "df"))

# Add empty sex specific rows --------------------------------------------------

events <- c("Arterial_event","Venous_event")
terms <- c("week1","week2","week3_4","week5_8","week9_12","week13_26","week27_49")

female <- data.frame(stratification = "Sex",
                     stratum = "Sex: Female",
                  event = rep(events, each = length(terms)), 
                  term = rep(terms, times = length(events)),
                  estimate = NA,
                  conf.low = NA,
                  conf.high = NA,
                  stringsAsFactors = FALSE)

male <- data.frame(stratification = "Sex",
                   stratum = "Sex: Male",
                     event = rep(events, each = length(terms)), 
                     term = rep(terms, times = length(events)),
                     estimate = NA,
                     conf.low = NA,
                     conf.high = NA,
                     stringsAsFactors = FALSE)

df <- rbind(df,female)
df <- rbind(df,male)
rm(list=setdiff(ls(), "df"))

# Save data --------------------------------------------------------------------

data.table::fwrite(df,"data/ccu002_01_figures_211001.csv")