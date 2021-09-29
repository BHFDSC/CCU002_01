# Databricks

This directory contains the Databricks notebooks used in the NHS Digital TRE for this project.

| Notebook | Author(s) | Description |
|-----------|-----------|-----------|
|CCU002_01-D00-LSOA_region_lookup | Sam Hollings | This notebook creates a LSOA to region lookup table.|
|CCU002_01-D00-project_table_freeze|Sam Hollings, Jenny Cooper, Rochelle Knight | This notebook extracts the data from specified time point (batchId) and then applies a specified common cutoff date (i.e. any records beyond this time are dropped).|
| CCU002_01-D01-codelist | Venexia Walker, Arun Karthikeyan Suseeladevi, Rochelle Knight | This notebook generates the codelists needed for this project.|
|CCU002_01-D03-patient_skinny_unassembled | Sam Hollings, Jenny Cooper, Rochelle Knight|Gather together the records for each patient in primary and secondary care before they are assembled into a skinny record for CCU002_01. The output of this is a global temp view which is then used by CCU002_01-D04-patient_skinny_assembled.|
|CCU002_01-D04-patient_skinny_assembled |Sam Hollings, Jenny Cooper, Rochelle Knight |Making a single record for each patient in primary and secondary care. This uses the output from CCU002_01_D03_patient_skinny_unassembled.|
|CCU002_01-D05-quality_assurance | Jenny Cooper, Sam Ip, Rochelle Knight | This notebook creates a register and applies a series of quality assurance steps to a cohort of data (Skinny Cohort table) of NHS Numbers to potentially remove from analyses due to conflicting data, with reference to previous work/coding by Spiros Denaxas.|
|CCU002_01-D06-inclusion_exclusion|Jenny Cooper, Sam Ip, Rochelle Knight|This notebook runs through the inclusion/exclusion criteria for the skinny cohort after QA.
|CCU002_01-D07-covariate_flags|Venexia Walker, Sam Ip, Spencer Keene, Rochelle Knight|This notebook creates a table of covariate flags using primary and secondary care records.|
|CCU002_01-D08-COVID_infections_temp_tables|Chris Tomlinson, Johan Thygesen (inspired by Sam Hollings)|This notebook creates tables for each of the main datasets required to create the COVID infections table.|
|CCU002_01-D09-COVID_trajectory|Johan Thygesen, Chris Tomlinson, Spiros Denaxas|This notebook identifies all patients with COVID-19 related diagnosis and creates a trajectory table with all data points for all affected individuals.|
|CCU002_01-D10-outcomes_primary_care_infection|Spencer Keene, Rachel Denholm|This notebook generates the outcomes listed in the CCU002_01 protocol using the GDPPR dataset.|
|CCU002_01-D11-outcomes_hes_infection|Spencer Keene, Rachel Denholm|This notebook generates the outcomes listed in the CCU002_01 protocol using the HES APC dataset.|
|CCU002_01-D12-outcomes_death_infection|Spencer Keene, Rachel Denholm|This notebook generates the outcomes listed in the CCU002_01 protocol using the Deaths dataset.|
|CCU002_01-D14-cohort|Venexia Walker, Sam Ip, Spencer Keene, Rochelle Knight|This notebook makes the analysis dataset.|
