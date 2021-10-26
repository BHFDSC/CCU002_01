rm(list = ls())

# Load figure 1 data -----------------------------------------------------------

df1 <- data.table::fread("data/ccu002_01_main_data_figures_1.csv", 
                         select = c("event","agegp","term","estimate","conf.low","conf.high","stratum","stratification"),
                         data.table = FALSE)

df1 <- df1[df1$agegp=="all" | df1$stratification=="Age group",]

df1 <- df1[df1$event %in% c("angina","HF","stroke_SAH_HS","stroke_TIA","AMI","DVT_event","PE","stroke_isch"),]
df1 <- df1[df1$stratification %in% c("Overall", "Hospitalised/Non-hospitalised COVID-19"),]
df1 <- df1[df1$agegp=="all",]
df1 <- df1[df1$term %in% c("week1","week2","week3_4","week5_8","week9_12","week13_26","week27_49"),]

# Load figures 2 and 3 data ----------------------------------------------------

df2 <- data.table::fread("data/ccu002_01_main_data_figures_2_3.csv", 
                         select = c("event","agegp","term","estimate","conf.low","conf.high","stratum","stratification","nation"),
                         data.table = FALSE)

df2 <- df2[df2$agegp=="all" | df2$stratification=="Age group",]

tmp <- unique(df2[,c("event","stratum","nation")])
tmp$present <- 1
tmp <- tidyr::pivot_wider(tmp, names_from = "nation", values_from = "present")
tmp$nation <- ifelse(is.na(tmp$all),"England","all")
tmp <- tmp[,c("event","stratum","nation")]
df2 <- merge(df2, tmp, by = c("event","stratum","nation"))
df2$nation <- NULL
df2$term <- ifelse(df2$term=="week1_4","week1",df2$term)
df2$term <- ifelse(df2$term=="week5_49","week5_8",df2$term)

# Combine all plot data --------------------------------------------------------

df <- rbind(df1,df2)

# Tidy event labels ------------------------------------------------------------

df$event <- ifelse(df$event=="Arterial_event","First arterial thrombosis",df$event)
df$event <- ifelse(df$event=="Venous_event","First venous thromboembolism",df$event)
df$event <- ifelse(df$event=="angina","Angina",df$event)
df$event <- ifelse(df$event=="HF","Heart failure",df$event)
df$event <- ifelse(df$event=="stroke_SAH_HS","Subarachnoid haemorrhage and haemorrhagic stroke",df$event)
df$event <- ifelse(df$event=="stroke_TIA","Transient ischaemic attack",df$event)
df$event <- ifelse(df$event=="AMI","Acute myocardial infarction",df$event)
df$event <- ifelse(df$event=="PE","Pulmonary embolism",df$event)
df$event <- ifelse(df$event=="stroke_isch","Ischaemic stroke",df$event)
df$event <- ifelse(df$event=="DVT_event","Deep vein thrombosis",df$event)
df$event <- ifelse(df$event=="other_arterial_embolism","Other arterial embolism",df$event)
df$event <- ifelse(df$event=="portal_vein_thrombosis","Portal vein thrombosis",df$event)
df$event <- ifelse(df$event=="ICVT_event","Intracranial venous thrombosis",df$event) 

# Derive table for each event --------------------------------------------------

#for (event in unique(df$event)) {
  
  # Restrict to estimates for event of interest --------------------------------
  
#  tmp <- df[df$event==event,]
  
  tmp <- df

  # Specify estimate format ----------------------------------------------------  
  
  tmp$est <- paste0(ifelse(tmp$estimate>=10,sprintf("%.1f",tmp$estimate),sprintf("%.2f",tmp$estimate)),
                    " (",ifelse(tmp$conf.low>=10,sprintf("%.1f",tmp$conf.low),sprintf("%.2f",tmp$conf.low)),
                    "-",ifelse(tmp$conf.high>=10,sprintf("%.1f",tmp$conf.high),sprintf("%.2f",tmp$conf.high)),")")
  
  # Remove unnecessary variables -----------------------------------------------
  
  tmp[,c("estimate","conf.low","conf.high","agegp","stratification")] <- NULL
  
  # Convert long to wide -------------------------------------------------------
  
  tmp <- tidyr::pivot_wider(tmp, names_from = term, values_from = est)
  
  # Specify estimate order -----------------------------------------------------
  
  tmp$event <- factor(tmp$event, levels=c("Acute myocardial infarction",
                                      "Ischaemic stroke",
                                      "Other arterial embolism",
                                      "Pulmonary embolism",
                                      "Deep vein thrombosis",
                                      "Intracranial venous thrombosis",
                                      "Portal vein thrombosis",
                                      "Other deep vein thrombosis",
                                      "Heart failure",
                                      "Angina",
                                      "Transient ischaemic attack",
                                      "Subarachnoid haemorrhage and haemorrhagic stroke",
                                      "First arterial thrombosis",
                                      "First venous thromboembolism")) 

  tmp$stratum <- factor(tmp$stratum, levels=c("Extensive adjustment",
                                              "Age/sex/region adjustment",
                                              "Hospitalised COVID-19",
                                              "Non-hospitalised COVID-19",
                                              "Prior history of event",
                                              "No prior history of event",
                                              "Age group: <40",
                                              "Age group: 40-59",
                                              "Age group: 60-79",
                                              "Age group: >=80",
                                              "Sex: Female",
                                              "Sex: Male",
                                              "Ethnicity: White",
                                              "Ethnicity: Black or Black British",
                                              "Ethnicity: Asian or Asian British",
                                              "Ethnicity: Other Ethnic Groups",
                                              "Ethnicity: Mixed")) 
  
  tmp <- tmp[order(tmp$event,tmp$stratum),]
  
  # Save -----------------------------------------------------------------------  
  
  #data.table::fwrite(tmp, paste0("output/ccu002_01_suppl_table_4_",event,".csv"))
  data.table::fwrite(tmp, paste0("output/ccu002_01_suppl_table_4.csv"))
  
#}