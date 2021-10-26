rm(list = ls())

# Load data --------------------------------------------------------------------

df <- readxl::read_excel("raw/event_counts/EventCounts_Overall_AllOutcomes.xlsx", 
                 sheet = "Event counts using DB notebook")

df[,c("agegp","events_total")] <- NULL
colnames(df) <- c("event","exposure","hospitalised","non_hospitalised")
df$exposure <- ifelse(df$exposure=="pre expo","pre",df$exposure)
df$exposure <- ifelse(df$exposure=="all post expo","post",df$exposure)

# Make columns for exposure time -----------------------------------------------

df <- tidyr::pivot_wider(df, names_from = "exposure", values_from = c("hospitalised","non_hospitalised"))

# Format column names ----------------------------------------------------------

df$non_hospitalised_pre <- NULL
colnames(df) <- c("Event","No COVID-19","Hospitalised COVID-19","Non-hospitalised COVID-19")

# Add empty rows ---------------------------------------------------------------

df <- rbind(df,c("Other vascular events",rep(NA,3)))
df <- rbind(df,c("Arterial thrombosis events",rep(NA,3)))
df <- rbind(df,c("Venous thromboembolism events",rep(NA,3)))

# Tidy event labels ------------------------------------------------------------

df$Event <- ifelse(df$Event=="Arterial_event","    First arterial thrombosis",df$Event)
df$Event <- ifelse(df$Event=="Venous_event","    First venous thromboembolism",df$Event)
df$Event <- ifelse(df$Event=="angina","    Angina",df$Event)
df$Event <- ifelse(df$Event=="HF","    Heart failure",df$Event)
df$Event <- ifelse(df$Event=="stroke_SAH_HS","    Subarachnoid haemorrhage and haemorrhagic stroke",df$Event)
df$Event <- ifelse(df$Event=="stroke_TIA","    Transient ischaemic attack",df$Event)
df$Event <- ifelse(df$Event=="AMI","    Acute myocardial infarction",df$Event)
df$Event <- ifelse(df$Event=="PE","    Pulmonary embolism",df$Event)
df$Event <- ifelse(df$Event=="stroke_isch","    Ischaemic stroke",df$Event)
df$Event <- ifelse(df$Event=="DVT_event","    Deep vein thrombosis",df$Event)
df$Event <- ifelse(df$Event=="other_arterial_embolism","    Other arterial embolism",df$Event)
df$Event <- ifelse(df$Event=="portal_vein_thrombosis","    Portal vein thrombosis",df$Event)
df$Event <- ifelse(df$Event=="ICVT_event","    Intracranial venous thrombosis",df$Event) 
df$Event <- ifelse(df$Event=="other_DVT","    Other deep vein thrombosis",df$Event) 

df$Event <- factor(df$Event, levels=c("Arterial thrombosis events",
                                      "    First arterial thrombosis",
                                      "    Acute myocardial infarction",
                                      "    Ischaemic stroke",
                                      "    Other arterial embolism",
                                      "Venous thromboembolism events",
                                      "    First venous thromboembolism",
                                      "    Pulmonary embolism",
                                      "    Deep vein thrombosis",
                                      "    Other deep vein thrombosis",
                                      "    Intracranial venous thrombosis",
                                      "    Portal vein thrombosis",
                                      "Other vascular events",
                                      "    Heart failure",
                                      "    Angina",
                                      "    Transient ischaemic attack",
                                      "    Subarachnoid haemorrhage and haemorrhagic stroke")) 

df <- df[order(df$Event),]

# Disclosure control -----------------------------------------------------------

df$`No COVID-19` <- ifelse(as.numeric(df$`No COVID-19`)<10,"<10",df$`No COVID-19`)
df$`Hospitalised COVID-19` <- ifelse(as.numeric(df$`Hospitalised COVID-19`)<10,"<10",df$`Hospitalised COVID-19`)
df$`Non-hospitalised COVID-19` <- ifelse(as.numeric(df$`Non-hospitalised COVID-19`)<10,"<10",df$`Non-hospitalised COVID-19`)

# Save -------------------------------------------------------------------------

data.table::fwrite(df,"output/ccu002_01_main_table_2.csv")