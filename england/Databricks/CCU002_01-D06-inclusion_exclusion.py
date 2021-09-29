# Databricks notebook source
# MAGIC %md # CCU002_01-D04-inclusion_exclusion
# MAGIC  
# MAGIC **Description** This notebook runs through the inclusion/exclusion criteria for the skinny cohort after QA.
# MAGIC 
# MAGIC **Author(s)** Updated by Rochelle Knight based on work by Jenny Cooper and Samantha Ip CCU002_02
# MAGIC 
# MAGIC **Reviewer(s)** 
# MAGIC 
# MAGIC **Date last updated** 
# MAGIC 
# MAGIC **Date last reviewed**
# MAGIC 
# MAGIC **Data last run** 
# MAGIC 
# MAGIC **Data input** `dars_nic_391419_j3w9t_collab.ccu002_01_inf_conflictingpatients`
# MAGIC 
# MAGIC `dars_nic_391419_j3w9t_collab.ccu002_01_sgss_dars_nic_391419_j3w9t`
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Data output** `dars_nic_391419_j3w9t_collab.ccu002_01_inf_included_patients`
# MAGIC 
# MAGIC **Software and versions** SQL, Python
# MAGIC 
# MAGIC **Packages and versions** Not applicable

# COMMAND ----------

%run Workspaces/dars_nic_391419_j3w9t_collab/CCU002_01/CCU002_01-functions/wrang000_functions


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Set up

# COMMAND ----------

# MAGIC %sql
# MAGIC --Run this only if updating tables
# MAGIC 
# MAGIC --DROP VIEW IF EXISTS global_temp.patientinclusion;
# MAGIC --DROP VIEW IF EXISTS global_temp.practicesover1;
# MAGIC --DROP VIEW IF EXISTS global_temp.patients_died;
# MAGIC --DROP VIEW IF EXISTS global_temp.positive_test;
# MAGIC --DROP VIEW IF EXISTS global_temp.nhs_icd_exclude;
# MAGIC --DROP VIEW IF EXISTS global_temp.nhs_snomed_exclude;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Infection parameters

# COMMAND ----------

#Dataset Parameters 
skinny_data = 'inf_skinny_patient'
sgss_data = 'sgss_dars_nic_391419_j3w9t'

#Other data inputs
conflicting_quality_assurance = 'inf_skinny_conflicting_patients'

#Date parameters
index_date = '2020-01-01'

#Final table name
collab_database_name = 'dars_nic_391419_j3w9t_collab'
project_prefix = 'ccu002_01_'
incl_excl_table_name = 'inf_included_patients' 

# COMMAND ----------

from pyspark.sql.functions import countDistinct, year, dayofmonth, from_unixtime, month, unix_timestamp, to_timestamp, date_format, col, datediff, to_date, lit, months_between
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql import Window
import io
from functools import reduce
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anti-join QA-excluded IDs to skinny table -- gives skinny_withQA

# COMMAND ----------

#antijoin QA table to skinny table to remove conflicting patients identified from QA
spark.sql(f"""
create or replace global temp view {project_prefix}skinny_withQA as

SELECT t1.*
FROM {collab_database_name}.{project_prefix}{skinny_data} t1
LEFT JOIN {collab_database_name}.{project_prefix}{conflicting_quality_assurance} t2 ---those who didnt meet QA criteria, from the previous notebook
ON t1.nhs_number_deid = t2.nhs_number_deid
WHERE t2.nhs_number_deid IS NULL """)


# COMMAND ----------

# DBTITLE 1,Creating Temporary Tables for the inclusion and exclusion criteria
 %py
 #People to include:
 #Known sex and age 18 and over -- people who have not died before 1st Jan 2020 and who have SEX==1/2 and who are over 18
 spark.sql(F"""create or replace global temp view {project_prefix}patientinclusion AS 
 
 SELECT *
 FROM global_temp.{project_prefix}skinny_withQA 
 WHERE nhs_number_deid is not null 
 AND AGE_AT_COHORT_START >=18 
 AND (SEX =1 OR SEX=2)
 AND ((DATE_OF_DEATH >= '{index_date}') or (DATE_OF_DEATH is null)) """)

# COMMAND ----------

 %py
 # Excluding those with a positive test prior to 1st Jan 2020 for vaccine work
 spark.sql(F"""create or replace global temp view {project_prefix}positive_test_pre_2020 AS   
 
 SELECT distinct(PERSON_ID_DEID) as nhs_number_deid --same nhs identifer name needed to union
 FROM {collab_database_name}.{project_prefix}{sgss_data}
 WHERE Lab_Report_date <'{index_date}' AND PERSON_ID_DEID is not null""")

# COMMAND ----------

 %py
 #anti-join inclusion population with exclusion NHS numbers
 spark.sql(f"""
 create or replace global temp view {project_prefix}{incl_excl_table_name} AS   
 
 select 
 NHS_NUMBER_DEID, SEX, CATEGORISED_ETHNICITY, ETHNIC, DATE_OF_BIRTH, DATE_OF_DEATH, AGE_AT_COHORT_START  --keep here if want to specify specific variables, otherwise not needed.
 FROM
 (
 SELECT t1.*
 FROM global_temp.{project_prefix}patientinclusion t1
 LEFT JOIN  
 
 (
 SELECT * FROM global_temp.{project_prefix}positive_test_pre_2020 --positive COVID test
 ) t2 
 ON t1.nhs_number_deid = t2.nhs_number_deid
 
 WHERE t2.nhs_number_deid IS NULL) """)
 
 #Gives a list of nhs numbers to include in the study

# COMMAND ----------

 %py
 
 spark.sql(F"""DROP TABLE IF EXISTS {collab_database_name}.{project_prefix}{incl_excl_table_name}""")


