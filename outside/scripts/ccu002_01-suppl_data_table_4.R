rm(list = ls())

# Load data --------------------------------------------------------------------

df <- data.table::fread("data/ccu002_01_main_data_figures_1_2.csv", 
                         select = c("event","agegp","term","estimate","conf.low","conf.high","stratum","stratification"),
                         data.table = FALSE)

df2 <- data.table::fread("data/ccu002_01_suppl_data_figures_1_2.csv", 
                         select = c("event","agegp","term","estimate","conf.low","conf.high","stratum","stratification"),
                         data.table = FALSE)

df3 <- data.table::fread("data/ccu002_01_suppl_data_figures_3.csv", 
                         select = c("event","agegp","term","estimate","conf.low","conf.high","stratum"),
                         data.table = FALSE)

df3$stratification <- "Overall"

df <- rbind(df,df2)
df <- rbind(df,df3)

df <- df[df$agegp=="all" | df$stratification=="Age group",]

# Derive table for each event --------------------------------------------------

for (event in unique(df$event)) {
  
  # Restrict to estimates for event of interest --------------------------------
  
  tmp <- df[df$event==event,]
  
  # Specify estimate format ----------------------------------------------------  
  
  tmp$est <- paste0(ifelse(tmp$estimate>=10,sprintf("%.1f",tmp$estimate),sprintf("%.2f",tmp$estimate)),
                    " (",ifelse(tmp$conf.low>=10,sprintf("%.1f",tmp$conf.low),sprintf("%.2f",tmp$conf.low)),
                    "-",ifelse(tmp$conf.high>=10,sprintf("%.1f",tmp$conf.high),sprintf("%.2f",tmp$conf.high)),")")
  
  # Remove unnecessary variables -----------------------------------------------
  
  tmp[,c("event","estimate","conf.low","conf.high","agegp","stratification")] <- NULL
  
  # Convert long to wide -------------------------------------------------------
  
  tmp <- tidyr::pivot_wider(tmp, names_from = term, values_from = est)
  
  # Specify estimate order -----------------------------------------------------
  
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
  
  tmp <- tmp[order(tmp$stratum),]
  
  # Save -----------------------------------------------------------------------  
  
  data.table::fwrite(tmp, paste0("output/ccu002_01_suppl_table_4_",event,".csv"))
  
}