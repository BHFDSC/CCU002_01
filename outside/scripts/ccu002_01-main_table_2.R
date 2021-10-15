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

df <- rbind(df,c("    Other deep vein thrombosis",rep(NA,3)))
df <- rbind(df,c("Seperate outcomes",rep(NA,3)))
df <- rbind(df,c("Arterial events",rep(NA,3)))
df <- rbind(df,c("Venous events",rep(NA,3)))

# Tidy event labels ------------------------------------------------------------

df$Event <- ifelse(df$Event=="Arterial_event","    Any arterial event",df$Event)
df$Event <- ifelse(df$Event=="Venous_event","    Any venous event",df$Event)
df$Event <- ifelse(df$Event=="angina","    Angina",df$Event)
df$Event <- ifelse(df$Event=="HF","    Heart failure",df$Event)
df$Event <- ifelse(df$Event=="stroke_SAH_HS","    Subarachnoid haemorrhage & hemorrhagic stroke",df$Event)
df$Event <- ifelse(df$Event=="stroke_TIA","    Transient ischaemic attack",df$Event)
df$Event <- ifelse(df$Event=="AMI","    Acute myocardial infarction",df$Event)
df$Event <- ifelse(df$Event=="PE","    Pulmonary embolism",df$Event)
df$Event <- ifelse(df$Event=="stroke_isch","    Ischaemic stroke",df$Event)
df$Event <- ifelse(df$Event=="DVT_event","    Deep vein thrombosis",df$Event)
df$Event <- ifelse(df$Event=="other_arterial_embolism","    Other arterial embolism",df$Event)
df$Event <- ifelse(df$Event=="portal_vein_thrombosis","    Portal vein thrombosis",df$Event)
df$Event <- ifelse(df$Event=="ICVT_event","    Intercranial venous thrombosis",df$Event) 

df$Event <- factor(df$Event, levels=c("Arterial events",
                                      "    Any arterial event",
                                      "    Acute myocardial infarction",
                                      "    Ischaemic stroke",
                                      "    Other arterial embolism",
                                      "Venous events",
                                      "    Any venous event",
                                      "    Pulmonary embolism",
                                      "    Deep vein thrombosis",
                                      "    Intercranial venous thrombosis",
                                      "    Portal vein thrombosis",
                                      "    Other deep vein thrombosis",
                                      "Seperate outcomes",
                                      "    Heart failure",
                                      "    Angina",
                                      "    Transient ischaemic attack",
                                      "    Subarachnoid haemorrhage & hemorrhagic stroke")) 

df <- df[order(df$Event),]

# Save -------------------------------------------------------------------------

data.table::fwrite(df,"output/ccu002_01_main_table_2.csv")