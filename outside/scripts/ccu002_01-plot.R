rm(list = ls())

# Load plot data ---------------------------------------------------------------

df <- data.table::fread("data/ccu002_01_figures_211001.csv", data.table = FALSE)

# Specify time -----------------------------------------------------------------

df$timeA <- as.numeric(gsub("_.*","",gsub("week","",df$term))) 
df$timeB <- as.numeric(gsub(".*_","",gsub("week","",df$term))) 
df$time <- (df$timeA+df$timeB)/2
df[,c("timeA","timeB")] <- NULL
  
# Specify line types -----------------------------------------------------------

df$dashed <- ifelse(df$stratum %in% c("Adjustment: age/sex/region",
                                      "Sex: Female",
                                      "Non-hospitalised COVID-19",
                                      "No history of event"),
                    "dashed","solid")

# Specify line colours ---------------------------------------------------------

df$colour <- ""
df$colour <- ifelse(df$stratification=="Adjustment","#000000",df$colour)
df$colour <- ifelse(df$stratum=="Age: <40","#a6cee3",df$colour)
df$colour <- ifelse(df$stratum=="Age: 40-59","#b2df8a",df$colour)
df$colour <- ifelse(df$stratum=="Age: 60-79","#fb9a99",df$colour)
df$colour <- ifelse(df$stratum=="Age: >=80","#fdbf6f",df$colour)
df$colour <- ifelse(df$stratum=="Sex: Male","#cab2d6",df$colour)
df$colour <- ifelse(df$stratum=="Sex: Female","#cab2d6",df$colour)
df$colour <- ifelse(df$stratum=="Ethnicity: White","#e41a1c",df$colour)
df$colour <- ifelse(df$stratum=="Ethnicity: Black","#1f78b4",df$colour)
df$colour <- ifelse(df$stratum=="Ethnicity: Asian","#33a02c",df$colour)
df$colour <- ifelse(df$stratum=="Ethnicity: Other","#6a3d9a",df$colour)
df$colour <- ifelse(df$stratum=="Ethnicity: Mixed","#ff7f00",df$colour)
df$colour <- ifelse(df$stratification=="Severity","#A9A9A9",df$colour)
df$colour <- ifelse(df$stratification=="History","#D3D3D3",df$colour)

# Prepare legend info ----------------------------------------------------------

tmp <- unique(df[,c("stratum","colour","dashed")])
tmp$stratum <- ifelse(tmp$stratum=="Adjustment: extensive","Adjustment (solid: extensive; dashed: age/sex/region)",tmp$stratum)
tmp$stratum <- ifelse(tmp$stratum=="Adjustment: age/sex/region","Adjustment (solid: extensive; dashed: age/sex/region)",tmp$stratum)
tmp$stratum <- ifelse(tmp$stratum=="Hospitalised COVID-19","Severity (solid: hospitalised; dashed: non-hospitalised)",tmp$stratum)
tmp$stratum <- ifelse(tmp$stratum=="Non-hospitalised COVID-19","Severity (solid: hospitalised; dashed: non-hospitalised)",tmp$stratum)
tmp$stratum <- ifelse(tmp$stratum=="No history of event","History of event (solid: yes; dashed: no)",tmp$stratum)
tmp$stratum <- ifelse(tmp$stratum=="History of event","History of event (solid: yes; dashed: no)",tmp$stratum)
tmp$stratum <- ifelse(tmp$stratum=="Sex: Female","Sex (solid: male; dashed: female)",tmp$stratum)
tmp$stratum <- ifelse(tmp$stratum=="Sex: Male","Sex (solid: male; dashed: female)",tmp$stratum)
legend_labels <- tmp$stratum
colour_values <- tmp$colour
line_values <- tmp$dashed

# Plot and save ----------------------------------------------------------

for (event in c("Arterial_event","Venous_event")) {
  
ggplot2::ggplot(data = df[df$event==event,], 
                mapping = ggplot2::aes(x = time, y = estimate, color = colour)) +
  ggplot2::geom_point(position = ggplot2::position_dodge(width = 1)) +
  ggplot2::geom_line(mapping = ggplot2::aes(linetype = dashed)) +
  ggplot2::geom_errorbar(mapping = ggplot2::aes(ymin = conf.low, ymax = conf.high,  width = 0), 
                         position = ggplot2::position_dodge(width = 1)) +
  ggplot2::geom_hline(mapping = ggplot2::aes(yintercept = 1), colour = "#A9A9A9") +
  ggplot2::scale_y_continuous(lim = c(0.25,16), breaks = c(0.25,0.5,1,2,4,8,16), trans = "log") +
  ggplot2::scale_x_continuous(lim = c(0,44), breaks = seq(0,44,4)) +
  ggplot2::scale_color_manual(breaks = colour_values, 
                              values = colour_values,
                              labels = legend_labels) +
  ggplot2::scale_linetype_manual(values = c("dashed","solid"), guide = "none") +
  ggplot2::labs(x = "\nWeeks since COVID-19 infection", y = "Hazard ratio and 95% confidence interval") +
  ggplot2::guides(color=ggplot2::guide_legend(ncol = 6, bycol = TRUE)) +
  ggplot2::theme_minimal() +
  ggplot2::theme(panel.grid.major.x = ggplot2::element_blank(),
                 panel.grid.minor = ggplot2::element_blank(),
                 #panel.background = ggplot2::element_rect(fill = "white"),
                 #strip.background = ggplot2::element_rect(fill = "white"),
                 legend.key = ggplot2::element_rect(colour = NA, fill = NA),
                 legend.title = ggplot2::element_blank(),
                 legend.position="bottom") +
  ggplot2::facet_wrap(stratification~.)
  
  ggplot2::ggsave(paste0("output/",event,"s.pdf"), height = 210, width = 297, unit = "mm", dpi = 600, scale = 1)
  
  
  ggplot2::ggplot(data = df[df$event==event,], 
                  mapping = ggplot2::aes(x = time, y = estimate, color = colour)) +
    ggplot2::geom_point(position = ggplot2::position_dodge(width = 1)) +
    ggplot2::geom_line(mapping = ggplot2::aes(linetype = dashed)) +
    ggplot2::geom_errorbar(mapping = ggplot2::aes(ymin = conf.low, ymax = conf.high,  width = 0), 
                           position = ggplot2::position_dodge(width = 1)) +
    ggplot2::geom_hline(mapping = ggplot2::aes(yintercept = 1), colour = "#A9A9A9") +
    ggplot2::scale_y_continuous(lim = c(0.25,64), breaks = c(0.25,0.5,1,2,4,8,16,32,64), trans = "log") +
    ggplot2::scale_x_continuous(lim = c(0,44), breaks = seq(0,44,4)) +
    ggplot2::scale_color_manual(breaks = colour_values, 
                                values = colour_values,
                                labels = legend_labels) +
    ggplot2::scale_linetype_manual(values = c("dashed","solid"), guide = "none") +
    ggplot2::labs(x = "\nWeeks since COVID-19 infection", y = "Hazard ratio and 95% confidence interval") +
    ggplot2::guides(color=ggplot2::guide_legend(ncol = 6, bycol = TRUE)) +
    ggplot2::theme_minimal() +
    ggplot2::theme(panel.grid.major.x = ggplot2::element_blank(),
                   panel.grid.minor = ggplot2::element_blank(),
                   #panel.background = ggplot2::element_rect(fill = "white"),
                   #strip.background = ggplot2::element_rect(fill = "white"),
                   legend.key = ggplot2::element_rect(colour = NA, fill = NA),
                   legend.title = ggplot2::element_blank(),
                   legend.position="bottom") +
    ggplot2::facet_wrap(stratification~.)
  
  ggplot2::ggsave(paste0("output/",event,"s_relaxedy.pdf"), height = 210, width = 297, unit = "mm", dpi = 600, scale = 1)
  
}