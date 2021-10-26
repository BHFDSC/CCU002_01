rm(list = ls())

# Load plot data ---------------------------------------------------------------

df <- data.table::fread("data/ccu002_01_main_data_figures_2_3.csv", 
                        select = c("event","agegp","term","estimate","conf.low","conf.high","stratum","stratification","source","nation"),
                        data.table = FALSE)

df <- df[df$agegp=="all" | df$stratification=="Age group",]

# Use meta-analysis across all nations where possible --------------------------

tmp <- unique(df[,c("event","stratum","nation")])
tmp$present <- 1
tmp <- tidyr::pivot_wider(tmp, names_from = "nation", values_from = "present")
tmp$nation <- ifelse(is.na(tmp$all),"England","all")
tmp <- tmp[,c("event","stratum","nation")]
df <- merge(df, tmp, by = c("event","stratum","nation"))

# Specify time -----------------------------------------------------------------

term_to_time <- data.frame(term = c("week1","week2","week3_4","week5_8","week9_12","week13_26","week27_49",
                                    "week1_4","week5_49"),
                           time = c(0.5,1.5,3,6,10,19,37.5,
                                    2,26.5))

df <- merge(df, term_to_time, by = c("term"), all.x = TRUE)

# Give ethnicity estimates extra space -----------------------------------------

df$time <- ifelse(df$stratum=="Ethnicity: Asian or Asian British", df$time-0.5, df$time)
df$time <- ifelse(df$stratum=="Ethnicity: Other Ethnic Groups", df$time-1.0, df$time)
df$time <- ifelse(df$stratum=="Ethnicity: Mixed", df$time+0.5, df$time)
df$time <- ifelse(df$stratum=="Ethnicity: Black or Black British", df$time+1.0, df$time)

# Specify line colours ---------------------------------------------------------

df$colour <- ""
df$colour <- ifelse(df$stratum=="Extensive adjustment","#000000",df$colour)
df$colour <- ifelse(df$stratum=="Age/sex/region adjustment","#bababa",df$colour)
df$colour <- ifelse(df$stratum=="Age group: <40","#006d2c",df$colour)
df$colour <- ifelse(df$stratum=="Age group: 40-59","#31a354",df$colour)
df$colour <- ifelse(df$stratum=="Age group: 60-79","#74c476",df$colour)
df$colour <- ifelse(df$stratum=="Age group: >=80","#bae4b3",df$colour)
df$colour <- ifelse(df$stratum=="Sex: Male","#cab2d6",df$colour)
df$colour <- ifelse(df$stratum=="Sex: Female","#6a3d9a",df$colour)
df$colour <- ifelse(df$stratum=="Ethnicity: White","#08519c",df$colour)
df$colour <- ifelse(df$stratum=="Ethnicity: Black or Black British","#2171b5",df$colour)
df$colour <- ifelse(df$stratum=="Ethnicity: Asian or Asian British","#4292c6",df$colour)
df$colour <- ifelse(df$stratum=="Ethnicity: Other Ethnic Groups","#6baed6",df$colour)
df$colour <- ifelse(df$stratum=="Ethnicity: Mixed","#9ecae1",df$colour)
df$colour <- ifelse(df$stratum=="Non-hospitalised COVID-19","#fb9a99",df$colour)
df$colour <- ifelse(df$stratum=="Hospitalised COVID-19","#e31a1c",df$colour)
df$colour <- ifelse(df$stratum=="Prior history of event","#ff7f00",df$colour)
df$colour <- ifelse(df$stratum=="No prior history of event","#fdbf6f",df$colour)

# Factor variables for ordering ------------------------------------------------

df$stratification <- factor(df$stratification, levels=c("Overall",
                                                        "Hospitalised/Non-hospitalised COVID-19",
                                                        "Prior history of event",
                                                        "Age group",
                                                        "Sex",
                                                        "Ethnicity")) 

df$stratum <- factor(df$stratum, levels=c("Extensive adjustment",
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

df$colour <- factor(df$colour, levels=c("#000000",
                                        "#bababa",
                                        "#e31a1c",
                                        "#fb9a99",
                                        "#ff7f00",
                                        "#fdbf6f",
                                        "#006d2c",
                                        "#31a354",
                                        "#74c476",
                                        "#bae4b3",
                                        "#6a3d9a",
                                        "#cab2d6",
                                        "#08519c",
                                        "#2171b5",
                                        "#4292c6",
                                        "#6baed6",
                                        "#9ecae1")) 

# Plot and save ----------------------------------------------------------------

min_plot <- 0.5
max_plot <- 64

for (event in c("Arterial_event","Venous_event")) {
  
  ggplot2::ggplot(data = df[df$event==event,], 
                  mapping = ggplot2::aes(x = time, y = estimate, color = stratum, shape = stratum, fill = stratum)) +
    ggplot2::geom_hline(mapping = ggplot2::aes(yintercept = 1), colour = "#A9A9A9") +
    ggplot2::geom_point(position = ggplot2::position_dodge(width = 1)) +
    ggplot2::geom_errorbar(mapping = ggplot2::aes(ymin = ifelse(conf.low<min_plot,min_plot,conf.low), 
                                                  ymax = ifelse(conf.high>max_plot,max_plot,conf.high),  
                                                  width = 0), 
                           position = ggplot2::position_dodge(width = 1)) +
    ggplot2::geom_line(position = ggplot2::position_dodge(width = 1)) +
    ggplot2::scale_y_continuous(lim = c(min_plot,max_plot), breaks = c(0.25,0.5,1,2,4,8,16,32,64,128), trans = "log") +
    ggplot2::scale_x_continuous(lim = c(0,44), breaks = seq(0,44,4)) +
    ggplot2::scale_fill_manual(values = levels(df$colour), labels = levels(df$stratum)) +
    ggplot2::scale_color_manual(values = levels(df$colour), labels = levels(df$stratum)) +
    ggplot2::scale_shape_manual(values = c(rep(c(21,22),4),23,24,rep(c(21,22),2),23,24,25), labels = levels(df$stratum)) +
    ggplot2::labs(x = "\nWeeks since COVID-19 diagnosis", y = "Hazard ratio and 95% confidence interval") +
    ggplot2::guides(fill=ggplot2::guide_legend(ncol=6,byrow=TRUE)) +
    ggplot2::theme_minimal() +
    ggplot2::theme(panel.grid.major.x = ggplot2::element_blank(),
                   panel.grid.minor = ggplot2::element_blank(),
                   legend.key = ggplot2::element_rect(colour = NA, fill = NA),
                   legend.title = ggplot2::element_blank(),
                   legend.position="bottom",
                   plot.background = ggplot2::element_rect(fill = "white", colour = "white")) +
    ggplot2::facet_wrap(stratification~.)
  
  ggplot2::ggsave(paste0("output/ccu002_01_main_figures_2_3_",tolower(gsub("_event","",event)),".png"), height = 210, width = 297, unit = "mm", dpi = 600, scale = 1)
  
}