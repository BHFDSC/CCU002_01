# Databricks notebook source
# MAGIC %md #HES APC: CVD outcomes 
# MAGIC **Description** This notebooks make a table CCU002_cvd_outcomes_hesapc which contains all observations with the relevant observations containing outcomes clinical codes from the HES APC dataset recorded from study start date **1st Jan 2020**. Phenotypes used are listed below:
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
# MAGIC **Data input** HES APC
# MAGIC 
# MAGIC **Data output table:** CCU002_01_cvd_outcomes_hesapc
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
 hes_data = 'dars_nic_391419_j3w9t_collab.ccu002_01_hes_apc_all_years'
 
 #Date parameters
 index_date = '2020-01-01'
 end_date = '2020-12-07'
 
 
 #Final table name
 final_table = 'ccu002_01_inf_outcomes_hes_final' 
  

# COMMAND ----------

%run Workspaces/dars_nic_391419_j3w9t_collab/CCU002_01/CCU002_01-functions/wrang000_functions


# COMMAND ----------

 %py
 
 spark.sql(F"""create or replace global temp view CCU002_01_infection_hesapc
 as select *
 from {hes_data}""") 

# COMMAND ----------

 %sql
 ----All relevent HES outcome codelists used
 
 create or replace global temp view CCU002_01_infection_hesoutcomes_map as
 
 SELECT LEFT ( REGEXP_REPLACE(code,'[.,-,' ']','') , 4 ) AS ICD10code_trunc,
 name, terminology, code, term, code_type, RecordDate
 FROM dars_nic_391419_j3w9t_collab.ccu002_01_codelist
 WHERE (name = 'AMI' OR name = 'HF' OR name = 'angina' OR name = 'stroke_TIA' OR name = 'stroke_isch' OR name = 'unstable_angina' OR name = 'DIC' OR name = 'DVT_DVT' OR name = 'DVT_ICVT' OR name = 'DVT_pregnancy' OR name = 'ICVT_pregnancy' OR name = 'PE' OR name = 'TTP' OR name = 'artery_dissect' OR name = 'cardiomyopathy' OR name = 'fracture' OR name = 'life_arrhythmia' OR name = 'mesenteric_thrombus' OR name = 'myocarditis' OR name = 'other_DVT' OR name = 'other_arterial_embolism' OR name = 'pericarditis' OR name = 'portal_vein_thrombosis' OR name = 'stroke_SAH_HS' OR name = 'thrombocytopenia' OR name = 'thrombophilia')
       AND terminology = 'ICD10' --AND code_type=1 AND RecordDate=20210127
       

# COMMAND ----------

 # MAGIC %md 
 ###First position

# COMMAND ----------

 %sql
 -------Patients with relevant CVD event codes in the HES APC dataset after 31st Jan 2020
 ---created truncated caliber codes with dot missing and using all outcomes
 
 CREATE TABLE dars_nic_391419_j3w9t_collab.CCU002_01_infection_hesapcoutcomes_first_diagnosis  AS
 --create or replace global temp view CCU002_infection_hesapcoutcomes_first_diagnosis as
 
 with cte_hes as (
 SELECT PERSON_ID_DEID, SPELBGIN, EPISTART, ADMIDATE, DISDATE, DIAG_3_CONCAT, DIAG_3_01, DIAG_3_02, 
 DIAG_3_03, DIAG_3_04, DIAG_3_05, DIAG_3_06, DIAG_3_07, DIAG_3_08, DIAG_3_09, DIAG_3_10, 
 DIAG_3_11, DIAG_3_12, DIAG_3_13, DIAG_3_14, DIAG_3_15, DIAG_3_16, DIAG_3_17, DIAG_3_18, 
 DIAG_3_19, DIAG_3_20, 
 DIAG_4_CONCAT, DIAG_4_01, DIAG_4_02, 
 DIAG_4_03, DIAG_4_04, DIAG_4_05, DIAG_4_06, DIAG_4_07, DIAG_4_08, DIAG_4_09, DIAG_4_10, 
 DIAG_4_11, DIAG_4_12, DIAG_4_13, DIAG_4_14, DIAG_4_15, DIAG_4_16, DIAG_4_17, DIAG_4_18, 
 DIAG_4_19, DIAG_4_20, 
  LEFT ( REGEXP_REPLACE(DIAG_4_01,'[X]$','') , 4 ) AS DIAG_4_01_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_02,'[X]$','') , 4 ) AS DIAG_4_02_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_03,'[X]$','') , 4 ) AS DIAG_4_03_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_04,'[X]$','') , 4 ) AS DIAG_4_04_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_05,'[X]$','') , 4 ) AS DIAG_4_05_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_06,'[X]$','') , 4 ) AS DIAG_4_06_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_07,'[X]$','') , 4 ) AS DIAG_4_07_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_08,'[X]$','') , 4 ) AS DIAG_4_08_trunc, 
  LEFT ( REGEXP_REPLACE(DIAG_4_09,'[X]$','') , 4 ) AS DIAG_4_09_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_10,'[X]$','') , 4 ) AS DIAG_4_10_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_11,'[X]$','') , 4 ) AS DIAG_4_11_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_12,'[X]$','') , 4 ) AS DIAG_4_12_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_13,'[X]$','') , 4 ) AS DIAG_4_13_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_14,'[X]$','') , 4 ) AS DIAG_4_14_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_15,'[X]$','') , 4 ) AS DIAG_4_15_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_16,'[X]$','') , 4 ) AS DIAG_4_16_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_17,'[X]$','') , 4 ) AS DIAG_4_17_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_18,'[X]$','') , 4 ) AS DIAG_4_18_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_19,'[X]$','') , 4 ) AS DIAG_4_19_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_20,'[X]$','') , 4 ) AS DIAG_4_20_trunc
 FROM global_temp.CCU002_01_infection_hesapc
 WHERE EPISTART > '2019-12-31' AND EPISTART < '2020-12-08'
 )
 
 select *
 from cte_hes t1
 inner join global_temp.CCU002_01_infection_hesoutcomes_map t2 on 
   (t1.DIAG_3_01 = t2.ICD10code_trunc
 /*OR t1.DIAG_3_02 = t2.ICD10code_trunc
 OR t1.DIAG_3_03 = t2.ICD10code_trunc
 OR t1.DIAG_3_04 = t2.ICD10code_trunc
 OR t1.DIAG_3_05 = t2.ICD10code_trunc
 OR t1.DIAG_3_06 = t2.ICD10code_trunc
 OR t1.DIAG_3_07 = t2.ICD10code_trunc
 OR t1.DIAG_3_08 = t2.ICD10code_trunc
 OR t1.DIAG_3_09 = t2.ICD10code_trunc
 OR t1.DIAG_3_10 = t2.ICD10code_trunc
 OR t1.DIAG_3_11 = t2.ICD10code_trunc
 OR t1.DIAG_3_12 = t2.ICD10code_trunc
 OR t1.DIAG_3_13 = t2.ICD10code_trunc
 OR t1.DIAG_3_14 = t2.ICD10code_trunc
 OR t1.DIAG_3_15 = t2.ICD10code_trunc
 OR t1.DIAG_3_16 = t2.ICD10code_trunc
 OR t1.DIAG_3_17 = t2.ICD10code_trunc
 OR t1.DIAG_3_18 = t2.ICD10code_trunc
 OR t1.DIAG_3_19 = t2.ICD10code_trunc
 OR t1.DIAG_3_20 = t2.ICD10code_trunc*/
 OR t1.DIAG_4_01_trunc = t2.ICD10code_trunc
 /*OR t1.DIAG_4_02_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_03_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_04_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_05_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_06_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_07_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_08_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_09_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_10_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_11_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_12_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_13_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_14_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_15_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_16_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_17_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_18_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_19_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_20_trunc = t2.ICD10code_trunc*/
 )

# COMMAND ----------

 # MAGIC %md 
 ###All diagnosis positions

# COMMAND ----------

 %sql
 -------Patients with relevant CVD event codes in the HES APC dataset after 31st Jan 2020
 ---created truncated caliber codes with dot missing and using all outcomes
 
 CREATE TABLE dars_nic_391419_j3w9t_collab.CCU002_01_infection_hesapcoutcomes  AS
 --create or replace global temp view CCU002_infection_hesapcoutcomes as
 
 with cte_hes as (
 SELECT PERSON_ID_DEID, SPELBGIN, EPISTART, ADMIDATE, DISDATE, DIAG_3_CONCAT, DIAG_3_01, DIAG_3_02, 
 DIAG_3_03, DIAG_3_04, DIAG_3_05, DIAG_3_06, DIAG_3_07, DIAG_3_08, DIAG_3_09, DIAG_3_10, 
 DIAG_3_11, DIAG_3_12, DIAG_3_13, DIAG_3_14, DIAG_3_15, DIAG_3_16, DIAG_3_17, DIAG_3_18, 
 DIAG_3_19, DIAG_3_20, 
 DIAG_4_CONCAT, DIAG_4_01, DIAG_4_02, 
 DIAG_4_03, DIAG_4_04, DIAG_4_05, DIAG_4_06, DIAG_4_07, DIAG_4_08, DIAG_4_09, DIAG_4_10, 
 DIAG_4_11, DIAG_4_12, DIAG_4_13, DIAG_4_14, DIAG_4_15, DIAG_4_16, DIAG_4_17, DIAG_4_18, 
 DIAG_4_19, DIAG_4_20, 
  LEFT ( REGEXP_REPLACE(DIAG_4_01,'[X]$','') , 4 ) AS DIAG_4_01_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_02,'[X]$','') , 4 ) AS DIAG_4_02_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_03,'[X]$','') , 4 ) AS DIAG_4_03_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_04,'[X]$','') , 4 ) AS DIAG_4_04_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_05,'[X]$','') , 4 ) AS DIAG_4_05_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_06,'[X]$','') , 4 ) AS DIAG_4_06_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_07,'[X]$','') , 4 ) AS DIAG_4_07_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_08,'[X]$','') , 4 ) AS DIAG_4_08_trunc, 
  LEFT ( REGEXP_REPLACE(DIAG_4_09,'[X]$','') , 4 ) AS DIAG_4_09_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_10,'[X]$','') , 4 ) AS DIAG_4_10_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_11,'[X]$','') , 4 ) AS DIAG_4_11_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_12,'[X]$','') , 4 ) AS DIAG_4_12_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_13,'[X]$','') , 4 ) AS DIAG_4_13_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_14,'[X]$','') , 4 ) AS DIAG_4_14_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_15,'[X]$','') , 4 ) AS DIAG_4_15_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_16,'[X]$','') , 4 ) AS DIAG_4_16_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_17,'[X]$','') , 4 ) AS DIAG_4_17_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_18,'[X]$','') , 4 ) AS DIAG_4_18_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_19,'[X]$','') , 4 ) AS DIAG_4_19_trunc,
  LEFT ( REGEXP_REPLACE(DIAG_4_20,'[X]$','') , 4 ) AS DIAG_4_20_trunc
 FROM global_temp.CCU002_01_infection_hesapc
 WHERE EPISTART > '2019-12-31' AND EPISTART < '2020-12-08'
 )
 
 select *
 from cte_hes t1
 inner join global_temp.CCU002_01_infection_hesoutcomes_map t2 on 
   (t1.DIAG_3_01 = t2.ICD10code_trunc
 OR t1.DIAG_3_02 = t2.ICD10code_trunc
 OR t1.DIAG_3_03 = t2.ICD10code_trunc
 OR t1.DIAG_3_04 = t2.ICD10code_trunc
 OR t1.DIAG_3_05 = t2.ICD10code_trunc
 OR t1.DIAG_3_06 = t2.ICD10code_trunc
 OR t1.DIAG_3_07 = t2.ICD10code_trunc
 OR t1.DIAG_3_08 = t2.ICD10code_trunc
 OR t1.DIAG_3_09 = t2.ICD10code_trunc
 OR t1.DIAG_3_10 = t2.ICD10code_trunc
 OR t1.DIAG_3_11 = t2.ICD10code_trunc
 OR t1.DIAG_3_12 = t2.ICD10code_trunc
 OR t1.DIAG_3_13 = t2.ICD10code_trunc
 OR t1.DIAG_3_14 = t2.ICD10code_trunc
 OR t1.DIAG_3_15 = t2.ICD10code_trunc
 OR t1.DIAG_3_16 = t2.ICD10code_trunc
 OR t1.DIAG_3_17 = t2.ICD10code_trunc
 OR t1.DIAG_3_18 = t2.ICD10code_trunc
 OR t1.DIAG_3_19 = t2.ICD10code_trunc
 OR t1.DIAG_3_20 = t2.ICD10code_trunc
 OR t1.DIAG_4_01_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_02_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_03_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_04_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_05_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_06_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_07_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_08_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_09_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_10_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_11_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_12_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_13_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_14_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_15_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_16_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_17_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_18_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_19_trunc = t2.ICD10code_trunc
 OR t1.DIAG_4_20_trunc = t2.ICD10code_trunc
 )

