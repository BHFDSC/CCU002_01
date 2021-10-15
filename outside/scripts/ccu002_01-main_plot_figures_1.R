rm(list = ls())

# Load plot data ---------------------------------------------------------------

df <- data.table::fread("data/ccu002_01_main_data_figures_1.csv", 
                        data.table = FALSE)

df <- df[df$event %in% c("angina","HF","stroke_SAH_HS","stroke_TIA","AMI","DVT_event","PE","stroke_isch"),]
df <- df[df$stratification %in% c("Overall", "Hospitalised/Non-hospitalised COVID-19"),]
df <- df[df$agegp=="all",]
df <- df[df$term %in% c("week1","week2","week3_4","week5_8","week9_12","week13_26","week27_49"),]

synthetic <- unique(df[df$event=="HF",c("event","agegp","sex","term")])
synthetic$stratification <-  "Hospitalised/Non-hospitalised COVID-19"
synthetic$source <- "synthetic"

synthetic$stratum <- "Non-hospitalised COVID-19"
df <- plyr::rbind.fill(df,synthetic)

synthetic$stratum <- "Hospitalised COVID-19"
df <- plyr::rbind.fill(df,synthetic)

# Specify time -----------------------------------------------------------------

term_to_time <- data.frame(term = c("week1","week2","week3_4","week5_8","week9_12","week13_26","week27_49",
                                    "week1_4","week5_49"),
                           time = c(0.5,1.5,3,6,10,19,37.5,
                                    2,26.5))

df <- merge(df, term_to_time, by = c("term"), all.x = TRUE)


# Specify line colours ---------------------------------------------------------

df$colour <- ""
df$colour <- ifelse(df$stratum=="Extensive adjustment","#000000",df$colour)
df$colour <- ifelse(df$stratum=="Age/sex/region adjustment","#bababa",df$colour)
df$colour <- ifelse(df$stratum=="Non-hospitalised COVID-19","#fb9a99",df$colour)
df$colour <- ifelse(df$stratum=="Hospitalised COVID-19","#e31a1c",df$colour)

# Factor variables for ordering-------------------------------------------------

df$stratification <- factor(df$stratification, levels=c("Overall",
                                                        "Hospitalised/Non-hospitalised COVID-19")) 

df$stratum <- factor(df$stratum, levels=c("Extensive adjustment",
                                          "Age/sex/region adjustment",
                                          "Hospitalised COVID-19",
                                          "Non-hospitalised COVID-19")) 

df$colour <- factor(df$colour, levels=c("#000000",
                                        "#bababa",
                                        "#e31a1c",
                                        "#fb9a99"))

# Make event names 'nice' ------------------------------------------------------

df$event <- ifelse(df$event=="angina","Angina",df$event)
df$event <- ifelse(df$event=="HF","Heart failure",df$event)
df$event <- ifelse(df$event=="stroke_SAH_HS","Subarachnoid haemorrhage & hemorrhagic stroke",df$event)
df$event <- ifelse(df$event=="stroke_TIA","Transient ischaemic attack",df$event)
df$event <- ifelse(df$event=="AMI","Acute myocardial infarction",df$event)
df$event <- ifelse(df$event=="PE","Pulmonary embolism",df$event)
df$event <- ifelse(df$event=="stroke_isch","Ischaemic stroke",df$event)
df$event <- ifelse(df$event=="DVT_event","Deep vein thrombosis",df$event)

df$event <- factor(df$event, levels=c("Acute myocardial infarction",
                                      "Angina",
                                      "Heart failure",
                                      "Ischaemic stroke",
                                      "Subarachnoid haemorrhage & hemorrhagic stroke",
                                      "Transient ischaemic attack",
                                      "Deep vein thrombosis",
                                      "Pulmonary embolism"))

# Plot and save ----------------------------------------------------------------

df$event_strat <- paste0(df$event,"\n",df$stratification)

df$event_strat <- factor(df$event_strat, levels = c("Acute myocardial infarction\nOverall",
                                                    "Acute myocardial infarction\nHospitalised/Non-hospitalised COVID-19",
                                                    "Angina\nOverall",
                                                    "Angina\nHospitalised/Non-hospitalised COVID-19",
                                                    "Ischaemic stroke\nOverall",
                                                    "Ischaemic stroke\nHospitalised/Non-hospitalised COVID-19",
                                                    "Heart failure\nOverall",
                                                    "Heart failure\nHospitalised/Non-hospitalised COVID-19",
                                                    "Subarachnoid haemorrhage & hemorrhagic stroke\nOverall",
                                                    "Subarachnoid haemorrhage & hemorrhagic stroke\nHospitalised/Non-hospitalised COVID-19",
                                                    "Deep vein thrombosis\nOverall",
                                                    "Deep vein thrombosis\nHospitalised/Non-hospitalised COVID-19",
                                                    "Transient ischaemic attack\nOverall",
                                                    "Transient ischaemic attack\nHospitalised/Non-hospitalised COVID-19",
                                                    "Pulmonary embolism\nOverall",
                                                    "Pulmonary embolism\nHospitalised/Non-hospitalised COVID-19"))

ggplot2::ggplot(data = df,
                mapping = ggplot2::aes(x = time, y = estimate, color = stratum, shape=stratum, fill=stratum)) +
  ggplot2::geom_hline(mapping = ggplot2::aes(yintercept = 1), colour = "#A9A9A9") +
  ggplot2::geom_point(position = ggplot2::position_dodge(width = 1)) +
  ggplot2::geom_errorbar(mapping = ggplot2::aes(ymin = ifelse(conf.low<0.25,0.25,conf.low), 
                                                ymax = ifelse(conf.high>64,64,conf.high),  
                                                width = 0), 
                         position = ggplot2::position_dodge(width = 1)) +
  ggplot2::geom_line(position = ggplot2::position_dodge(width = 1)) +
  ggplot2::scale_y_continuous(lim = c(0.25,64), breaks = c(0.5,0.5,1,2,4,8,16,32,64), trans = "log") +
  ggplot2::scale_x_continuous(lim = c(0,44), breaks = seq(0,44,4)) +
  ggplot2::scale_fill_manual(values = levels(df$colour), labels = levels(df$stratum)) +
  ggplot2::scale_color_manual(values = levels(df$colour), labels = levels(df$stratum)) +
  ggplot2::scale_shape_manual(values = c(rep(c(21,22),3),23,24), labels = levels(df$stratum)) +
  ggplot2::labs(x = "\nWeeks since COVID-19 diagnosis", y = "Hazard ratio and 95% confidence interval") +
  ggplot2::guides(fill=ggplot2::guide_legend(ncol = 4, byrow = TRUE)) +
  ggplot2::theme_minimal() +
  ggplot2::theme(panel.grid.major.x = ggplot2::element_blank(),
                 panel.grid.minor = ggplot2::element_blank(),
                 panel.spacing.x = ggplot2::unit(0.5, "lines"),
                 panel.spacing.y = ggplot2::unit(0, "lines"),
                 legend.key = ggplot2::element_rect(colour = NA, fill = NA),
                 legend.title = ggplot2::element_blank(),
                 legend.position="bottom",
                 plot.background = ggplot2::element_rect(fill = "white", colour = "white")) +
  ggplot2::facet_wrap(event_strat~., ncol = 4)

ggplot2::ggsave(paste0("output/ccu002_01_main_figure1.png"), height = 210, width = 297, unit = "mm", dpi = 600, scale = 1)
