# Databricks notebook source
# MAGIC %md #Primary care: CVD outcomes 
# MAGIC 
# MAGIC **Description** This notebooks make a table `CCU002_cvd_outcomes_gdppr` which contains all observations with the relevant observations containing outcomes clinical codes from the GDPPR dataset, where events happened from start date **1st Jan 2020**. Phenotypes used are listed below:
# MAGIC 
# MAGIC 
# MAGIC **Project(s)** CCU002_01
# MAGIC  
# MAGIC **Author(s)** Spencer Keene adapted from Rachel Denholm's notebooks
# MAGIC  
# MAGIC **Reviewer(s)** 
# MAGIC  
# MAGIC **Date last updated** 
# MAGIC  
# MAGIC **Date last reviewed** 
# MAGIC  
# MAGIC **Date last run** 
# MAGIC  
# MAGIC **Data input** GDPPR
# MAGIC 
# MAGIC **Data output** table: `CCU002_01_cvd_outcomes_gdppr`
# MAGIC 
# MAGIC **Software and versions** SQL
# MAGIC  
# MAGIC **Packages and versions** 'Not applicable'

# COMMAND ----------

 # MAGIC %md
 #####Infection parameters

# COMMAND ----------

 %py
 
 #Dataset parameters
 gdppr_data = 'dars_nic_391419_j3w9t_collab.ccu002_01_gdppr_dars_nic_391419_j3w9t'
 
 #Date parameters
 index_date = '2020-01-01'
 end_date = '2020-12-07'
 
 #Final table name
 final_table ='ccu002_01_inf_outcomes_gdppr_final'   

# COMMAND ----------

 %py
 #Creating global temp for primary care data
 spark.sql(F"""create or replace global temp view CCU002_01_gdppr_infection
 as select *
 from {gdppr_data}""") 

# COMMAND ----------

 %sql
 ----Relevant outcomes codes
 
 create or replace global temp view CCU002_01_infection_gdpproutcomes_map as
 
 SELECT name, terminology, code, term, code_type, RecordDate
 FROM dars_nic_391419_j3w9t_collab.ccu002_01_codelist
 WHERE (name = 'AMI' OR name = 'HF' OR name = 'angina' OR name = 'stroke_TIA' OR name = 'stroke_isch' OR name = 'unstable_angina')
       AND terminology = 'SNOMED' --AND code_type=1

# COMMAND ----------

 %py
 #Patients with relevant CVD event codes in the GDPPR dataset after 31st Jan 2020
 
 spark.sql(F"""CREATE TABLE dars_nic_391419_j3w9t_collab.CCU002_01_infection_cvd_outcomes_gdppr AS
 
 with cte_gdppr as (
 SELECT NHS_NUMBER_DEID, DATE, CODE as snomed
 FROM global_temp.CCU002_01_gdppr_infection
 WHERE DATE >= {index_date} AND DATE <= {end_date} )
  
 select *
 from cte_gdppr t1
 inner join global_temp.CCU002_01_infection_gdpproutcomes_map t2 on t1.snomed = t2.code""") 

# COMMAND ----------

 %sql
 ---Patients with relevant CVD event codes in the GDPPR dataset after 31st Jan 2020
 
 spark.sql(F"""CREATE TABLE dars_nic_391419_j3w9t_collab.CCU002_01_infection_cvd_outcomes_gdppr AS
 
 with cte_gdppr as (
 SELECT NHS_NUMBER_DEID, DATE, CODE as snomed
 FROM global_temp.CCU002_gdppr_infection
 WHERE DATE >= {index_date} AND DATE <= {end_date}
 )
  
 select *
 from cte_gdppr t1
 inner join global_temp.CCU002_infection_gdpproutcomes_map t2 on t1.snomed = t2.code""") 


