# Databricks notebook source
# MAGIC %md # CCU002_01-D02-patient_skinny_assembled
# MAGIC 
# MAGIC **Description** Making a single record for each patient in primary and secondary care. This uses the output from *CCU002_01_D01_patient_skinny_unassembled*
# MAGIC  
# MAGIC **Author(s)** Updated by Rochelle Knight for CCU002_01 from work by Sam Hollings and Jenny Cooper for CCU002_02 
# MAGIC 
# MAGIC **Description** Populates the table `dars_nic_391419_j3w9t_collab.ccu002_01_inf_skinny_patient` for all patients (alive/dead) on 1st Jan 2020:
# MAGIC Uses records from GDPPR and HES, but only from **before 1st Jan 2020**. All death records are used. Multiple records for each patient are reconciled into a single version of the truth using the following algorithm:
# MAGIC - Non-NULL (including coded NULLs) taken first
# MAGIC - Primary (GDPPR) data is preferred next
# MAGIC - Finally, most recent record is chosen.
# MAGIC - Patient must also have a record in GDPPR
# MAGIC 
# MAGIC The records are taken from the `global_temp.patient_skinny_unassembled` which is made in notebook `https://db.core.data.digital.nhs.uk/#notebook/2694567/command/2694568` -
# MAGIC `CCU002_01-D01-patient_skinny_unassembled` This is simply a list of all the records for each patient from all the datasets.
# MAGIC  
# MAGIC **Project(s)** CCU002_01
# MAGIC  
# MAGIC **Reviewer(s)** 
# MAGIC  
# MAGIC **Date last updated** 
# MAGIC  
# MAGIC **Date last reviewed** 
# MAGIC  
# MAGIC **Date last run** 
# MAGIC  
# MAGIC **Data input** `global_temp.patient_skinny_unassembled` 
# MAGIC 
# MAGIC `dars_nic_391419_j3w9t_collab.ccu002_01_gdppr_dars_nic_391419_j3w9t`
# MAGIC 
# MAGIC **Data output** `dars_nic_391419_j3w9t_collab.ccu002_01_inf_skinny_patient`
# MAGIC 
# MAGIC **Software and versions** SQL, Python
# MAGIC  
# MAGIC **Packages and versions** Not applicable

# COMMAND ----------

# MAGIC %md ## Making a single record for each patient in primary and secondary care


# COMMAND ----------

# MAGIC %run Workspaces/dars_nic_391419_j3w9t_collab/CCU002_01/CCU002_01-functions/wrang000_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Infection Parameters

# COMMAND ----------

#Dataset Parameters (needed for the GDPPR presence lookup)
gdppr_data = 'gdppr_dars_nic_391419_j3w9t'
collab_database_name = 'dars_nic_391419_j3w9t_collab'


#Date Parameters
index_date = '2020-01-01' #For infection study

#Data output name
project_prefix = 'ccu002_01_'
skinny_table_name = 'inf_skinny_patient' 


# COMMAND ----------

# MAGIC %md ### Run dependency if not exists

# COMMAND ----------

dbutils.notebook.run("/Workspaces/dars_nic_391419_j3w9t_collab/CCU002_01/CCU002_01-D01-patient_skinny_unassembled",3000)

# COMMAND ----------

#Mark records before or after index date

spark.sql(
f"""CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}patient_skinny_unassembled_after_index as
SELECT *, 
CASE WHEN RECORD_DATE >= '{index_date}' THEN True ELSE False END as after_index
FROM global_temp.{project_prefix}patient_skinny_unassembled""")

# COMMAND ----------

# MAGIC %md ## Handle the multiple versions of the truth

# COMMAND ----------

# MAGIC %md Choose the appropriate values so that we have one version of the truth for each Patient.
# MAGIC 
# MAGIC Currently, that simply involves picking the **most recent record** (before 1st January 2020):
# MAGIC - choosing a populated field first
# MAGIC - choosing from primary care first if possible 
# MAGIC -  and then only choosing from secondary or null values if no other available).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}patient_fields_ranked_pre_index AS
SELECT * --NHS_NUMBER_DEID, DATE_OF_DEATH
FROM (
      SELECT *, 
                row_number() OVER (PARTITION BY NHS_NUMBER_DEID 
                                    ORDER BY date_of_birth_null asc, primary desc, RECORD_DATE DESC) as birth_recency_rank,
                row_number() OVER (PARTITION BY NHS_NUMBER_DEID 
                                    ORDER BY sex_null asc, primary desc, RECORD_DATE DESC) as sex_recency_rank,
                row_number() OVER (PARTITION BY NHS_NUMBER_DEID 
                                    ORDER BY ethnic_null asc, primary desc, RECORD_DATE DESC) as ethnic_recency_rank
                              
      FROM global_temp.{project_prefix}patient_skinny_unassembled_after_index
      WHERE (after_index = False) --or death_table = 1 --JC added this we want only records before index date but all deaths in case recorded after index date
      and dataset  <> "primary_SNOMED" -- <- this has the GDPPR SNOMED Ethnicity - we will just use the normal ones, which are in dataset = primary
      ) """)
       

# COMMAND ----------

# Get deaths after index as well as before
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}patient_fields_ranked_death AS
SELECT *
FROM (
      SELECT *, 
                row_number() OVER (PARTITION BY NHS_NUMBER_DEID 
                                    ORDER BY death_table desc,  RECORD_DATE DESC) as death_recency_rank
                              
      FROM global_temp.{project_prefix}patient_skinny_unassembled_after_index
      ) """)

# COMMAND ----------

# MAGIC %md You can check the Ranking of the records below - it will keep only those with `recency_rank = 1` later

# COMMAND ----------

# MAGIC %md ### Assemble the columns together to make skinny record

# COMMAND ----------

# MAGIC %md GDPPR Presence Lookup to only keep those in GDPPR

# COMMAND ----------

#Add a presence lookup
#(Could alternatively use presence code in previous notebook)

spark.sql(f"""CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}gdppr_presence_lookup AS

SELECT distinct(NHS_NUMBER_DEID)
FROM {collab_database_name}.{project_prefix}{gdppr_data}""")

# COMMAND ----------

# MAGIC %md Ethnicity lookup

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}ethnicity_lookup AS
SELECT *, 
      CASE WHEN ETHNICITY_CODE IN ('1','2','3','N','M','P') THEN "Black or Black British"
           WHEN ETHNICITY_CODE IN ('0','A','B','C') THEN "White"
           WHEN ETHNICITY_CODE IN ('4','5','6','L','K','J','H') THEN "Asian or Asian British"
           WHEN ETHNICITY_CODE IN ('7','8','W','T','S','R') THEN "Other Ethnic Groups"
           WHEN ETHNICITY_CODE IN ('D','E','F','G') THEN "Mixed"
           WHEN ETHNICITY_CODE IN ('9','Z','X') THEN "Unknown"
           ELSE 'Unknown' END as ETHNIC_GROUP  
FROM (
  SELECT ETHNICITY_CODE, ETHNICITY_DESCRIPTION FROM dss_corporate.hesf_ethnicity
  UNION ALL
  SELECT Value as ETHNICITY_CODE, Label as ETHNICITY_DESCRIPTION FROM dss_corporate.gdppr_ethnicity WHERE Value not in (SELECT ETHNICITY_CODE FROM FROM dss_corporate.hesf_ethnicity)) """)

# COMMAND ----------

# MAGIC %md assemble it all together:

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}patient_skinny_record AS
SELECT pat.NHS_NUMBER_DEID,
      eth.ETHNIC,
      eth_group.ETHNIC_GROUP as CATEGORISED_ETHNICITY,
      sex.SEX,
      dob.DATE_OF_BIRTH,
      dod.DATE_OF_DEATH
FROM (SELECT DISTINCT NHS_NUMBER_DEID FROM global_temp.{project_prefix}patient_skinny_unassembled_after_index) pat 
        INNER JOIN global_temp.{project_prefix}gdppr_presence_lookup pres ON pat.NHS_NUMBER_DEID = pres.NHS_NUMBER_DEID --added to ensure in GDPPR only as inner join, move this to the top for more efficiency
        INNER JOIN (SELECT NHS_NUMBER_DEID, ETHNIC FROM global_temp.{project_prefix}patient_fields_ranked_pre_index WHERE ethnic_recency_rank = 1) eth ON pat.NHS_NUMBER_DEID = eth.NHS_NUMBER_DEID
        INNER JOIN (SELECT NHS_NUMBER_DEID, SEX FROM global_temp.{project_prefix}patient_fields_ranked_pre_index WHERE sex_recency_rank = 1) sex ON pat.NHS_NUMBER_DEID = sex.NHS_NUMBER_DEID
        INNER JOIN (SELECT NHS_NUMBER_DEID, DATE_OF_BIRTH FROM global_temp.{project_prefix}patient_fields_ranked_pre_index WHERE birth_recency_rank = 1) dob ON pat.NHS_NUMBER_DEID = dob.NHS_NUMBER_DEID
        LEFT JOIN (SELECT NHS_NUMBER_DEID, DATE_OF_DEATH FROM global_temp.{project_prefix}patient_fields_ranked_death WHERE death_recency_rank = 1 and death_table = 1) dod ON pat.NHS_NUMBER_DEID = dod.NHS_NUMBER_DEID --deaths come just from death_table in this case
        LEFT JOIN global_temp.{project_prefix}ethnicity_lookup eth_group ON eth.ETHNIC = eth_group.ETHNICITY_CODE """)       


# COMMAND ----------

# Add age at cohort start

import pyspark.sql.functions as f
(spark.table(f" global_temp.{project_prefix}patient_skinny_record")
      .selectExpr("*", 
                  f"floor(float(months_between('{index_date}', DATE_OF_BIRTH))/12.0) as AGE_AT_COHORT_START")
      .createOrReplaceGlobalTempView(f"{project_prefix}patient_skinny_record_age"))

