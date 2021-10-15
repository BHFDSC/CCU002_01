rm(list = ls())

# Load data --------------------------------------------------------------------

df <- readxl::read_excel("raw/event_counts/EventCounts_Overall_AllOutcomes.xlsx", 
                 sheet = "Phenotype")

df[,c("agegp")] <- NULL

# Make columns for exposure time -----------------------------------------------

df <- tidyr::pivot_wider(df, names_from = "expo_week", values_from = c("Hospitalised","Non-hospitalised"))

# Format column names ----------------------------------------------------------

df$`Non-hospitalised_pre expo` <- NULL
colnames(df) <- c("Event","No COVID-19","Hospitalised COVID-19","Non-hospitalised COVID-19")

# # Calculate other events -------------------------------------------------------
# 
# df <- rbind(df, c("Other arterial events",
#                   df[df$Event=="Arterial_event",]$`No COVID-19` - (df[df$Event=="AMI",]$`No COVID-19` + df[df$Event=="stroke_isch",]$`No COVID-19`),
#                   df[df$Event=="Arterial_event",]$`Hospitalised COVID-19` - (df[df$Event=="AMI",]$`Hospitalised COVID-19` + df[df$Event=="stroke_isch",]$`Hospitalised COVID-19`),
#                   df[df$Event=="Arterial_event",]$`Non-hospitalised COVID-19` - (df[df$Event=="AMI",]$`Non-hospitalised COVID-19` + df[df$Event=="stroke_isch",]$`Non-hospitalised COVID-19`)))
# 
# df$`No COVID-19` <- as.numeric(df$`No COVID-19`)
# df$`Hospitalised COVID-19` <- as.numeric(df$`Hospitalised COVID-19`)
# df$`Non-hospitalised COVID-19` <- as.numeric(df$`Non-hospitalised COVID-19`)
# 
# df <- rbind(df, c("Other venous events",
#                   df[df$Event=="Venous_event",]$`No COVID-19` - (df[df$Event=="PE",]$`No COVID-19` + df[df$Event=="DVT_event",]$`No COVID-19`),
#                   df[df$Event=="Venous_event",]$`Hospitalised COVID-19` - (df[df$Event=="PE",]$`Hospitalised COVID-19` + df[df$Event=="DVT_event",]$`Hospitalised COVID-19`),
#                   df[df$Event=="Venous_event",]$`Non-hospitalised COVID-19` - (df[df$Event=="PE",]$`Non-hospitalised COVID-19` + df[df$Event=="DVT_event",]$`Non-hospitalised COVID-19`)))
# 
# df$`No COVID-19` <- as.numeric(df$`No COVID-19`)
# df$`Hospitalised COVID-19` <- as.numeric(df$`Hospitalised COVID-19`)
# df$`Non-hospitalised COVID-19` <- as.numeric(df$`Non-hospitalised COVID-19`)

# Tidy event labels ------------------------------------------------------------

df$Event <- ifelse(df$Event=="Arterial_event","Any arterial event",df$Event)
df$Event <- ifelse(df$Event=="Venous_event","Any venous event",df$Event)
df$Event <- ifelse(df$Event=="AMI","Acute myocardial infarction",df$Event)
df$Event <- ifelse(df$Event=="PE","Pulmonary embolism",df$Event)
df$Event <- ifelse(df$Event=="stroke_isch","Ischaemic stroke",df$Event)
df$Event <- ifelse(df$Event=="DVT_event","Deep vein thrombosis",df$Event)

df$Event <- factor(df$Event, levels=c("Any arterial event",
                                      "Acute myocardial infarction",
                                      "Ischaemic stroke",
                                      "Other arterial events",
                                      "Any venous event",
                                      "Deep vein thrombosis",
                                      "Pulmonary embolism",
                                      "Other venous events")) 

df <- df[order(df$Event),]
# Save -------------------------------------------------------------------------

data.table::fwrite(df,"output/ccu002_01_main_table_2.csv")