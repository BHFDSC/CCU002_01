rm(list = ls())

# Load plot data ---------------------------------------------------------------

df <- data.table::fread("data/ccu002_01_suppl_figure_estimates.csv", 
                        select = c("event","agegp","term","estimate","conf.low","conf.high","stratum","stratification","source"),
                        data.table = FALSE)

synthetic <- data.frame(event = rep(c("DVT_event"),each = 2),
                        agegp = "all",
                        term = "week1",
                        estimate = NA,
                        conf.low = NA,
                        conf.high = NA,
                        stratum = c("Hospitalised COVID-19","Non-hospitalised COVID-19"),
                        stratification = "Hospitalised/Non-hospitalised COVID-19",
                        source = "synthetic")

df <- rbind(df,synthetic)

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
df$colour <- ifelse(df$stratum=="Age group: <40","#006d2c",df$colour)
df$colour <- ifelse(df$stratum=="Age group: 40-59","#31a354",df$colour)
df$colour <- ifelse(df$stratum=="Age group: 60-79","#74c476",df$colour)
df$colour <- ifelse(df$stratum=="Age group: >=80","#bae4b3",df$colour)

# Factor variables for ordering-------------------------------------------------

df$stratification <- factor(df$stratification, levels=c("Overall",
                                                        "Hospitalised/Non-hospitalised COVID-19",
                                                        "Age group")) 

df$stratum <- factor(df$stratum, levels=c("Extensive adjustment",
                                          "Age/sex/region adjustment",
                                          "Hospitalised COVID-19",
                                          "Non-hospitalised COVID-19",
                                          "Age group: <40",
                                          "Age group: 40-59",
                                          "Age group: 60-79",
                                          "Age group: >=80")) 

df$colour <- factor(df$colour, levels=c("#000000",
                                        "#bababa",
                                        "#e31a1c",
                                        "#fb9a99",
                                        "#006d2c",
                                        "#31a354",
                                        "#74c476",
                                        "#bae4b3")) 

# Define umbrella variable for grouping outcomes--------------------------------

df$umbrella <- NA
for (rows in 1:length(df$event)){
  if ((df$event[rows]=="AMI")|(df$event[rows]=="stroke_isch")){
    df$umbrella[rows]="Arterial"
  } else if ((df$event[rows]=="DVT_event")|(df$event[rows]=="PE")){
    df$umbrella[rows]="Venous"
  }
}

df$umbrella <- factor(df$umbrella, levels=c("Arterial","Venous"))

# Make event names 'nice' ------------------------------------------------------

df$event <- ifelse(df$event=="AMI","Acute myocardial infarction",df$event)
df$event <- ifelse(df$event=="PE","Pulmonary embolism",df$event)
df$event <- ifelse(df$event=="stroke_isch","Ischaemic stroke",df$event)
df$event <- ifelse(df$event=="DVT_event","Deep vein thrombosis",df$event)

# Plot and save ----------------------------------------------------------------

for (umb in c("Arterial","Venous")){       
  
  ggplot2::ggplot(data = df[df$umbrella==umb,], 
                  mapping = ggplot2::aes(x = time, y = estimate, color = stratum, shape=stratum, fill=stratum)) +
    ggplot2::geom_hline(mapping = ggplot2::aes(yintercept = 1), colour = "#A9A9A9") +
    ggplot2::geom_point(position = ggplot2::position_dodge(width = 1)) +
    ggplot2::geom_errorbar(mapping = ggplot2::aes(ymin = ifelse(conf.low<0.25,0.25,conf.low), 
                                                  ymax = ifelse(conf.high>64,64,conf.high),  
                                                  width = 0), 
                           position = ggplot2::position_dodge(width = 1)) +
    ggplot2::geom_line(position = ggplot2::position_dodge(width = 1)) +
    ggplot2::scale_y_continuous(lim = c(0.25,64), breaks = c(0.25,0.5,1,2,4,8,16,32,64), trans = "log") +
    ggplot2::scale_x_continuous(lim = c(0,44), breaks = seq(0,44,4)) +
    ggplot2::scale_fill_manual(values = levels(df$colour), labels = levels(df$stratum)) +
    ggplot2::scale_color_manual(values = levels(df$colour), labels = levels(df$stratum)) +
    ggplot2::scale_shape_manual(values = c(rep(c(21,22),3),23,24), labels = levels(df$stratum)) +
    ggplot2::labs(x = "\nWeeks since COVID-19 diagnosis", y = "Hazard ratio and 95% confidence interval") +
    ggplot2::guides(fill=ggplot2::guide_legend(ncol = 4, byrow = TRUE)) +
    ggplot2::theme_minimal() +
    ggplot2::theme(panel.grid.major.x = ggplot2::element_blank(),
                   panel.grid.minor = ggplot2::element_blank(),
                   legend.key = ggplot2::element_rect(colour = NA, fill = NA),
                   legend.title = ggplot2::element_blank(),
                   legend.position="bottom",
                   plot.background = ggplot2::element_rect(fill = "white", colour = "white")) +
    ggplot2::facet_wrap(event~stratification)
  
    ggplot2::ggsave(paste0("output/suppl_figure_",tolower(umb),".png"), height = 210, width = 297, unit = "mm", dpi = 600, scale = 1)
  
}