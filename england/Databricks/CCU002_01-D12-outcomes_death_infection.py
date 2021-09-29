# Databricks notebook source
# MAGIC %md #Death: CVD outcomes (work package 2.5)
# MAGIC **Description** This notebooks make a table ccu002_death_outcomes which contains all observations with the relevant observations containing outcomes clinical codes from the death dataset recorded after **31st Jan 2020**. Phenotypes used are listed below:
# MAGIC 
# MAGIC 
# MAGIC Where multiple enteries for a person, use the most recent death date
# MAGIC 
# MAGIC **Project(s)** CCU002_01
# MAGIC 
# MAGIC **Author(s)** Spencer Keene adapted from Rachel Denholm's notebooks
# MAGIC 
# MAGIC **Reviewer(s)**
# MAGIC 
# MAGIC **Date last updated** 2021-01-26
# MAGIC 
# MAGIC **Date last reviewed**
# MAGIC 
# MAGIC **Date last run**
# MAGIC 
# MAGIC **Data input** HES APC
# MAGIC 
# MAGIC **Data output table:** ccu002_death_outcomes
# MAGIC 
# MAGIC **Software and versions** SQL
# MAGIC 
# MAGIC **Packages and versions** 'Not applicable'

# COMMAND ----------

%run Workspaces/dars_nic_391419_j3w9t_collab/CCU002_01/CCU002_01-functions/wrang000_functions

# COMMAND ----------

 # MAGIC %md 
 #####Infection parameters

# COMMAND ----------

 %py
 
 #Dataset parameters
 deaths_data = 'dars_nic_391419_j3w9t_collab.ccu002_01_deaths_dars_nic_391419_j3w9t'
 
 #Date parameters
 index_date = '2020-01-01'
 end_date = '2020-12-07'
 
 
 #Final table name
 final_table = 'ccu002_01_inf_outcomes_deaths_final' 

# COMMAND ----------

 %py
 
 spark.sql(F"""create or replace global temp view ccu002_01_infection_deaths
 as select *
 from {deaths_data}""") 

 %sql
 ----All HES codelists used
 
 create or replace global temp view ccu002_01_infection_deaths_outcomes_map as
 
 SELECT LEFT ( REGEXP_REPLACE(code,'[.,-,' ']','') , 4 ) AS ICD10code_trunc,
 name, terminology, code, term, code_type, RecordDate
 FROM dars_nic_391419_j3w9t_collab.ccu002_01_codelist
 WHERE (name = 'AMI' OR name = 'HF' OR name = 'angina' OR name = 'stroke_TIA' OR name = 'stroke_isch' OR name = 'unstable_angina' OR name = 'DIC' OR name = 'DVT_DVT' OR name = 'DVT_ICVT' OR name = 'DVT_pregnancy' OR name = 'ICVT_pregnancy' OR name = 'PE' OR name = 'TTP' OR name = 'artery_dissect' OR name = 'cardiomyopathy' OR name = 'fracture' OR name = 'life_arrhythmia' OR name = 'mesenteric_thrombus' OR name = 'myocarditis' OR name = 'other_DVT' OR name = 'other_arterial_embolism' OR name = 'pericarditis' OR name = 'portal_vein_thrombosis' OR name = 'stroke_SAH_HS' OR name = 'thrombocytopenia' OR name = 'thrombophilia')
       AND terminology = 'ICD10' --AND code_type=1 AND RecordDate=20210127
       

# COMMAND ----------

 # MAGIC %md 
 ###First diagnosis positions

# COMMAND ----------

 %sql
 ---Using latest death date - latest date of death from latest registered date
 ---created truncated caliber codes with dot missing and using all outcomes
 CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_01_infection_death_outcomes_first_position  AS
 --create or replace global temp view ccu002_infection_death_outcomes_first_position as 
 
 with cte_anydeath as (
 SELECT *
 FROM
   (select DEC_CONF_NHS_NUMBER_CLEAN_DEID as ID, REG_DATE_OF_DEATH_FORMATTED,
    row_number() OVER(PARTITION BY DEC_CONF_NHS_NUMBER_CLEAN_DEID
                          ORDER BY REG_DATE desc, REG_DATE_OF_DEATH_FORMATTED desc) as death_rank
   FROM global_temp.ccu002_01_infection_deaths
 WHERE REG_DATE_OF_DEATH_FORMATTED > '2019-12-31' AND REG_DATE_OF_DEATH_FORMATTED < '2020-12-08'
 --AND REG_DATE_OF_DEATH_FORMATTED <=current_date()
   )
 ),
 
 cte_anydeath_latest as (
 select *
 from cte_anydeath where death_rank = 1
 ),
 
 cte_deathcvd as (
 SELECT * 
 FROM 
   (SELECT DEC_CONF_NHS_NUMBER_CLEAN_DEID, REG_DATE_OF_DEATH_FORMATTED, S_UNDERLYING_COD_ICD10, S_COD_CODE_1,
   S_COD_CODE_2, S_COD_CODE_3, S_COD_CODE_4, S_COD_CODE_5, S_COD_CODE_6, S_COD_CODE_7, S_COD_CODE_8,
   S_COD_CODE_9, S_COD_CODE_10, S_COD_CODE_11, S_COD_CODE_12, S_COD_CODE_13, S_COD_CODE_14, S_COD_CODE_15,
    LEFT ( REGEXP_REPLACE(S_UNDERLYING_COD_ICD10,'[X]$','') , 3 ) AS S_UNDERLYING_COD_ICD10_trunc,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_1,'[X]$','') , 3 ) AS S_COD_CODE_1_trunc,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_2,'[X]$','') , 3 ) AS S_COD_CODE_2_trunc,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_3,'[X]$','') , 3 ) AS S_COD_CODE_3_trunc,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_4,'[X]$','') , 3 ) AS S_COD_CODE_4_trunc,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_5,'[X]$','') , 3 ) AS S_COD_CODE_5_trunc,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_6,'[X]$','') , 3 ) AS S_COD_CODE_6_trunc,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_7,'[X]$','') , 3 ) AS S_COD_CODE_7_trunc,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_8,'[X]$','') , 3 ) AS S_COD_CODE_8_trunc,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_9,'[X]$','') , 3 ) AS S_COD_CODE_9_trunc,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_10,'[X]$','') , 3 ) AS S_COD_CODE_10_trunc,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_11,'[X]$','') , 3 ) AS S_COD_CODE_11_trunc,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_12,'[X]$','') , 3 ) AS S_COD_CODE_12_trunc,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_13,'[X]$','') , 3 ) AS S_COD_CODE_13_trunc,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_14,'[X]$','') , 3 ) AS S_COD_CODE_14_trunc,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_15,'[X]$','') , 3 ) AS S_COD_CODE_15_trunc,
    LEFT ( REGEXP_REPLACE(S_UNDERLYING_COD_ICD10,'[X]$','') , 4 ) AS S_UNDERLYING_COD_ICD10_trunc_4,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_1,'[X]$','') , 4 ) AS S_COD_CODE_1_trunc_4,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_2,'[X]$','') , 4 ) AS S_COD_CODE_2_trunc_4,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_3,'[X]$','') , 4 ) AS S_COD_CODE_3_trunc_4,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_4,'[X]$','') , 4 ) AS S_COD_CODE_4_trunc_4,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_5,'[X]$','') , 4 ) AS S_COD_CODE_5_trunc_4,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_6,'[X]$','') , 4 ) AS S_COD_CODE_6_trunc_4,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_7,'[X]$','') , 4 ) AS S_COD_CODE_7_trunc_4,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_8,'[X]$','') , 4 ) AS S_COD_CODE_8_trunc_4,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_9,'[X]$','') , 4 ) AS S_COD_CODE_9_trunc_4,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_10,'[X]$','') , 4 ) AS S_COD_CODE_10_trunc_4,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_11,'[X]$','') , 4 ) AS S_COD_CODE_11_trunc_4,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_12,'[X]$','') , 4 ) AS S_COD_CODE_12_trunc_4,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_13,'[X]$','') , 4 ) AS S_COD_CODE_13_trunc_4,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_14,'[X]$','') , 4 ) AS S_COD_CODE_14_trunc_4,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_15,'[X]$','') , 4 ) AS S_COD_CODE_15_trunc_4
   FROM global_temp.ccu002_01_infection_deaths
   WHERE REG_DATE_OF_DEATH_FORMATTED > '2019-12-31' AND REG_DATE_OF_DEATH_FORMATTED < '2020-12-08'
   )
 ),
 
 cte_deathcvd_link as (
 select *
 from cte_deathcvd t1
 inner join global_temp.ccu002_01_infection_deaths_outcomes_map t2 on 
 (t1.S_UNDERLYING_COD_ICD10_trunc = t2.ICD10code_trunc
 /*OR t1.S_COD_CODE_1_trunc = t2.ICD10code_trunc
 OR t1.S_COD_CODE_2_trunc = t2.ICD10code_trunc
 OR t1.S_COD_CODE_3_trunc = t2.ICD10code_trunc
 OR t1.S_COD_CODE_4_trunc = t2.ICD10code_trunc
 OR t1.S_COD_CODE_5_trunc = t2.ICD10code_trunc
 OR t1.S_COD_CODE_6_trunc = t2.ICD10code_trunc
 OR t1.S_COD_CODE_7_trunc = t2.ICD10code_trunc
 OR t1.S_COD_CODE_8_trunc = t2.ICD10code_trunc
 OR t1.S_COD_CODE_9_trunc = t2.ICD10code_trunc
 OR t1.S_COD_CODE_10_trunc = t2.ICD10code_trunc
 OR t1.S_COD_CODE_11_trunc = t2.ICD10code_trunc
 OR t1.S_COD_CODE_12_trunc = t2.ICD10code_trunc
 OR t1.S_COD_CODE_13_trunc = t2.ICD10code_trunc
 OR t1.S_COD_CODE_14_trunc = t2.ICD10code_trunc
 OR t1.S_COD_CODE_15_trunc = t2.ICD10code_trunc*/
 OR t1.S_UNDERLYING_COD_ICD10_trunc_4 = t2.ICD10code_trunc
 /*OR t1.S_COD_CODE_1_trunc_4 = t2.ICD10code_trunc
 OR t1.S_COD_CODE_2_trunc_4 = t2.ICD10code_trunc
 OR t1.S_COD_CODE_3_trunc_4 = t2.ICD10code_trunc
 OR t1.S_COD_CODE_4_trunc_4 = t2.ICD10code_trunc
 OR t1.S_COD_CODE_5_trunc_4 = t2.ICD10code_trunc
 OR t1.S_COD_CODE_6_trunc_4 = t2.ICD10code_trunc
 OR t1.S_COD_CODE_7_trunc_4 = t2.ICD10code_trunc
 OR t1.S_COD_CODE_8_trunc_4 = t2.ICD10code_trunc
 OR t1.S_COD_CODE_9_trunc_4 = t2.ICD10code_trunc
 OR t1.S_COD_CODE_10_trunc_4 = t2.ICD10code_trunc
 OR t1.S_COD_CODE_11_trunc_4 = t2.ICD10code_trunc
 OR t1.S_COD_CODE_12_trunc_4 = t2.ICD10code_trunc
 OR t1.S_COD_CODE_13_trunc_4 = t2.ICD10code_trunc
 OR t1.S_COD_CODE_14_trunc_4 = t2.ICD10code_trunc
 OR t1.S_COD_CODE_15_trunc_4 = t2.ICD10code_trunc*/
 )
 )
 
 
 --cte_next as (
 select tab1.ID, tab1.REG_DATE_OF_DEATH_FORMATTED, tab2.DEC_CONF_NHS_NUMBER_CLEAN_DEID, tab2.name, tab2.term, tab2.ICD10code_trunc as code 
 from cte_anydeath_latest tab1
 left join cte_deathcvd_link tab2 on tab1.ID = tab2.DEC_CONF_NHS_NUMBER_CLEAN_DEID 
 --)
 
 --select *
 --from cte_next



# COMMAND ----------

 # MAGIC %md 
 ###All diagnosis positions

# COMMAND ----------

 %sql
 ---Using latest death date - latest date of death from latest registered date
 ---created truncated caliber codes with dot missing and using all outcomes
 CREATE TABLE dars_nic_391419_j3w9t_collab.ccu002_01_infection_death_outcomes  AS
 --create or replace global temp view ccu002_01_infection_death_outcomes as 
 
 with cte_anydeath as (
 SELECT *
 FROM
   (select DEC_CONF_NHS_NUMBER_CLEAN_DEID as ID, REG_DATE_OF_DEATH_FORMATTED,
    row_number() OVER(PARTITION BY DEC_CONF_NHS_NUMBER_CLEAN_DEID
                          ORDER BY REG_DATE desc, REG_DATE_OF_DEATH_FORMATTED desc) as death_rank
   FROM global_temp.ccu002_01_infection_deaths
 WHERE REG_DATE_OF_DEATH_FORMATTED > '2019-12-31' AND REG_DATE_OF_DEATH_FORMATTED < '2020-12-08'
 --AND REG_DATE_OF_DEATH_FORMATTED <=current_date()
   )
 ),
 
 cte_anydeath_latest as (
 select *
 from cte_anydeath where death_rank = 1
 ),
 
 cte_deathcvd as (
 SELECT * 
 FROM 
   (SELECT DEC_CONF_NHS_NUMBER_CLEAN_DEID, REG_DATE_OF_DEATH_FORMATTED, S_UNDERLYING_COD_ICD10, S_COD_CODE_1,
   S_COD_CODE_2, S_COD_CODE_3, S_COD_CODE_4, S_COD_CODE_5, S_COD_CODE_6, S_COD_CODE_7, S_COD_CODE_8,
   S_COD_CODE_9, S_COD_CODE_10, S_COD_CODE_11, S_COD_CODE_12, S_COD_CODE_13, S_COD_CODE_14, S_COD_CODE_15,
    LEFT ( REGEXP_REPLACE(S_UNDERLYING_COD_ICD10,'[X]$','') , 3 ) AS S_UNDERLYING_COD_ICD10_trunc,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_1,'[X]$','') , 3 ) AS S_COD_CODE_1_trunc,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_2,'[X]$','') , 3 ) AS S_COD_CODE_2_trunc,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_3,'[X]$','') , 3 ) AS S_COD_CODE_3_trunc,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_4,'[X]$','') , 3 ) AS S_COD_CODE_4_trunc,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_5,'[X]$','') , 3 ) AS S_COD_CODE_5_trunc,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_6,'[X]$','') , 3 ) AS S_COD_CODE_6_trunc,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_7,'[X]$','') , 3 ) AS S_COD_CODE_7_trunc,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_8,'[X]$','') , 3 ) AS S_COD_CODE_8_trunc,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_9,'[X]$','') , 3 ) AS S_COD_CODE_9_trunc,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_10,'[X]$','') , 3 ) AS S_COD_CODE_10_trunc,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_11,'[X]$','') , 3 ) AS S_COD_CODE_11_trunc,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_12,'[X]$','') , 3 ) AS S_COD_CODE_12_trunc,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_13,'[X]$','') , 3 ) AS S_COD_CODE_13_trunc,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_14,'[X]$','') , 3 ) AS S_COD_CODE_14_trunc,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_15,'[X]$','') , 3 ) AS S_COD_CODE_15_trunc,
    LEFT ( REGEXP_REPLACE(S_UNDERLYING_COD_ICD10,'[X]$','') , 4 ) AS S_UNDERLYING_COD_ICD10_trunc_4,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_1,'[X]$','') , 4 ) AS S_COD_CODE_1_trunc_4,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_2,'[X]$','') , 4 ) AS S_COD_CODE_2_trunc_4,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_3,'[X]$','') , 4 ) AS S_COD_CODE_3_trunc_4,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_4,'[X]$','') , 4 ) AS S_COD_CODE_4_trunc_4,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_5,'[X]$','') , 4 ) AS S_COD_CODE_5_trunc_4,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_6,'[X]$','') , 4 ) AS S_COD_CODE_6_trunc_4,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_7,'[X]$','') , 4 ) AS S_COD_CODE_7_trunc_4,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_8,'[X]$','') , 4 ) AS S_COD_CODE_8_trunc_4,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_9,'[X]$','') , 4 ) AS S_COD_CODE_9_trunc_4,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_10,'[X]$','') , 4 ) AS S_COD_CODE_10_trunc_4,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_11,'[X]$','') , 4 ) AS S_COD_CODE_11_trunc_4,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_12,'[X]$','') , 4 ) AS S_COD_CODE_12_trunc_4,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_13,'[X]$','') , 4 ) AS S_COD_CODE_13_trunc_4,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_14,'[X]$','') , 4 ) AS S_COD_CODE_14_trunc_4,
    LEFT ( REGEXP_REPLACE(S_COD_CODE_15,'[X]$','') , 4 ) AS S_COD_CODE_15_trunc_4
   FROM global_temp.ccu002_01_infection_deaths
   WHERE REG_DATE_OF_DEATH_FORMATTED > '2019-12-31' AND REG_DATE_OF_DEATH_FORMATTED < '2020-12-08'
   )
 ),
 
 cte_deathcvd_link as (
 select *
 from cte_deathcvd t1
 inner join global_temp.ccu002_infection_deaths_outcomes_map t2 on 
 (t1.S_UNDERLYING_COD_ICD10_trunc = t2.ICD10code_trunc
 OR t1.S_COD_CODE_1_trunc = t2.ICD10code_trunc
 OR t1.S_COD_CODE_2_trunc = t2.ICD10code_trunc
 OR t1.S_COD_CODE_3_trunc = t2.ICD10code_trunc
 OR t1.S_COD_CODE_4_trunc = t2.ICD10code_trunc
 OR t1.S_COD_CODE_5_trunc = t2.ICD10code_trunc
 OR t1.S_COD_CODE_6_trunc = t2.ICD10code_trunc
 OR t1.S_COD_CODE_7_trunc = t2.ICD10code_trunc
 OR t1.S_COD_CODE_8_trunc = t2.ICD10code_trunc
 OR t1.S_COD_CODE_9_trunc = t2.ICD10code_trunc
 OR t1.S_COD_CODE_10_trunc = t2.ICD10code_trunc
 OR t1.S_COD_CODE_11_trunc = t2.ICD10code_trunc
 OR t1.S_COD_CODE_12_trunc = t2.ICD10code_trunc
 OR t1.S_COD_CODE_13_trunc = t2.ICD10code_trunc
 OR t1.S_COD_CODE_14_trunc = t2.ICD10code_trunc
 OR t1.S_COD_CODE_15_trunc = t2.ICD10code_trunc
 OR t1.S_UNDERLYING_COD_ICD10_trunc_4 = t2.ICD10code_trunc
 OR t1.S_COD_CODE_1_trunc_4 = t2.ICD10code_trunc
 OR t1.S_COD_CODE_2_trunc_4 = t2.ICD10code_trunc
 OR t1.S_COD_CODE_3_trunc_4 = t2.ICD10code_trunc
 OR t1.S_COD_CODE_4_trunc_4 = t2.ICD10code_trunc
 OR t1.S_COD_CODE_5_trunc_4 = t2.ICD10code_trunc
 OR t1.S_COD_CODE_6_trunc_4 = t2.ICD10code_trunc
 OR t1.S_COD_CODE_7_trunc_4 = t2.ICD10code_trunc
 OR t1.S_COD_CODE_8_trunc_4 = t2.ICD10code_trunc
 OR t1.S_COD_CODE_9_trunc_4 = t2.ICD10code_trunc
 OR t1.S_COD_CODE_10_trunc_4 = t2.ICD10code_trunc
 OR t1.S_COD_CODE_11_trunc_4 = t2.ICD10code_trunc
 OR t1.S_COD_CODE_12_trunc_4 = t2.ICD10code_trunc
 OR t1.S_COD_CODE_13_trunc_4 = t2.ICD10code_trunc
 OR t1.S_COD_CODE_14_trunc_4 = t2.ICD10code_trunc
 OR t1.S_COD_CODE_15_trunc_4 = t2.ICD10code_trunc
 )
 )
 
 select tab1.ID, tab1.REG_DATE_OF_DEATH_FORMATTED, tab2.DEC_CONF_NHS_NUMBER_CLEAN_DEID, tab2.name, tab2.term, tab2.ICD10code_trunc as code 
 from cte_anydeath_latest tab1
 left join cte_deathcvd_link tab2 on tab1.ID = tab2.DEC_CONF_NHS_NUMBER_CLEAN_DEID 

