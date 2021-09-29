# Databricks notebook source
# MAGIC %md
# MAGIC  # CCU002_01-D09-COVID_trajectory
# MAGIC  
# MAGIC **Description** 
# MAGIC 
# MAGIC This notebook:
# MAGIC * Identifies all patients with COVID-19 related diagnosis
# MAGIC * Creates a *trajectory* table with all data points for all affected individuals
# MAGIC   
# MAGIC 
# MAGIC **Project(s)** CCU002_01
# MAGIC  
# MAGIC **Author(s)** Johan Thygesen, Chris Tomlinson, Spiros Denaxas
# MAGIC  
# MAGIC **Reviewer(s)** 
# MAGIC  
# MAGIC **Date last updated** 
# MAGIC  
# MAGIC **Date last reviewed** 
# MAGIC  
# MAGIC **Date last run** 
# MAGIC  
# MAGIC **Data input**  
# MAGIC All inputs are via notebook `CCU002_01-D08-COVID_infections_temp_tables`
# MAGIC 
# MAGIC **Data output**
# MAGIC 
# MAGIC * `ccu002_01_covid_trajectory``
# MAGIC 
# MAGIC **Software and versions** SQL, python
# MAGIC  
# MAGIC **Packages and versions** See cell below:

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load functions and input data

# COMMAND ----------

%run "/Workspaces/dars_nic_391419_j3w9t_collab/CCU002_01/CCU002_01-D15-master_notebook_parameters"

# COMMAND ----------

#Dataset parameters
project_prefix = 'ccu002_01_'
collab_database_name = 'dars_nic_391419_j3w9t_collab' 
gdppr_data = 'gdppr_dars_nic_391419_j3w9t'
hes_apc_data = 'hes_apc_all_years'
hes_op_data = 'hes_op_all_years'
hes_ae_data = 'hes_ae_all_years'
hes_cc_data = 'hes_cc_all_years'
deaths_data = 'deaths_dars_nic_391419_j3w9t'
sgss_data = 'sgss_dars_nic_391419_j3w9t'
chess_data = 'chess_dars_nic_391419_j3w9t'
sus_data = 'sus_dars_nic_391419_j3w9t'
codelist_final_name = 'codelist'


#Date parameters
index_date = '2020-01-01' 

#Temp tables
temp_sgss = 'tmp_sgss'
temp_gdppr = 'tmp_gdppr'
temp_deaths = 'tmp_deaths'
temp_hes_apc = 'tmp_hes_apc'
temp_hes_cc = 'tmp_hes_cc'
temp_chess = 'tmp_chess'
temp_sus = 'tmp_sus'


#Final tables
temp_trajectory = 'tmp_covid_trajectory'
trajectory_final = 'covid_trajectory'




# COMMAND ----------

from pyspark.sql.functions import lit, col, udf
from functools import reduce
from pyspark.sql import DataFrame
from datetime import datetime
from pyspark.sql.types import DateType

# COMMAND ----------

# To reload the creation of the global temp tables run this line
dbutils.widgets.removeAll()

# COMMAND ----------

%run /Workspaces/dars_nic_391419_j3w9t_collab/CCU002_01/CCU002_01-functions/CCU002_01_COVID_trajectory_helper_functions_from_CCU013


# COMMAND ----------

#SGSS table
#all records are included as every record is a "positive test"
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}sgss_covid
AS
SELECT person_id_deid, date, 
"01_Covid_positive_test" as covid_phenotype, 
"" as clinical_code, 
CASE WHEN REPORTING_LAB_ID = '840' THEN "pillar_2" ELSE "pillar_1" END as description,
"confirmed" as covid_status,
"" as code,
"SGSS" as source, date_is
FROM {collab_database_name}.{project_prefix}{temp_sgss} """)

# COMMAND ----------

#GDPPR 
#Only includes individuals with a COVID SNOMED CODE

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}gdppr_covid as
with cte_snomed_covid_codes as (
select code, term
from {collab_database_name}.{project_prefix}{codelist_final_name}
where name like 'GDPPR_confirmed_COVID' ),

cte_gdppr as (
SELECT tab2.person_id_deid, tab2.date, tab2.code, tab2.date_is, tab1.term
FROM cte_snomed_covid_codes tab1
inner join {collab_database_name}.{project_prefix}{temp_gdppr} tab2 on tab1.code = tab2.code
)

SELECT person_id_deid, date, 
"01_GP_covid_diagnosis" as covid_phenotype,
code as clinical_code, 
term as description,
"confirmed" as covid_status, 
"SNOMED" as code, 
"GDPPR" as source, date_is 
from cte_gdppr """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Covid Admission

# COMMAND ----------

#SUS - Hospitalisations (COVID in any position)
#Get all patients hospitalised with a covid diagnosis U07.1 or U07.2
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}sus_covid_any_position as
SELECT person_id_deid, date, 
"02_Covid_admission_any_position" as covid_phenotype,
(case when DIAG_CONCAT LIKE "%U071%" THEN 'U07.1'
when DIAG_CONCAT LIKE "%U072%" THEN 'U07.2' Else '0' End) as clinical_code,
(case when DIAG_CONCAT LIKE "%U071%" THEN 'Confirmed_COVID19'
when DIAG_CONCAT LIKE "%U072%" THEN 'Suspected_COVID19' Else '0' End) as description,
(case when DIAG_CONCAT LIKE "%U071%" THEN 'confirmed'
when DIAG_CONCAT LIKE "%U072%" THEN 'suspected' Else '0' End) as covid_status,
"ICD10" as code,
"SUS" as source, 
date_is
FROM {collab_database_name}.{project_prefix}{temp_sus}
WHERE DIAG_CONCAT LIKE "%U071%"
   OR DIAG_CONCAT LIKE "%U072%" """)

# COMMAND ----------

#SUS - Hospitalisations (COVID in primary position)
#Get all patients hospitalised with a covid diagnosis U07.1 or U07.2
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}sus_covid_primary_position as
SELECT person_id_deid, date, 
"02_Covid_admission_primary_position" as covid_phenotype,
(case when PRIMARY_DIAGNOSIS_CODE LIKE "%U071%" THEN 'U07.1'
when PRIMARY_DIAGNOSIS_CODE LIKE "%U072%" THEN 'U07.2' Else '0' End) as clinical_code,
(case when PRIMARY_DIAGNOSIS_CODE LIKE "%U071%" THEN 'Confirmed_COVID19'
when PRIMARY_DIAGNOSIS_CODE LIKE "%U072%" THEN 'Suspected_COVID19' Else '0' End) as description,
(case when PRIMARY_DIAGNOSIS_CODE LIKE "%U071%" THEN 'confirmed'
when PRIMARY_DIAGNOSIS_CODE LIKE "%U072%" THEN 'suspected' Else '0' End) as covid_status,
"ICD10" as code,
"SUS" as source, 
date_is
FROM {collab_database_name}.{project_prefix}{temp_sus}
WHERE PRIMARY_DIAGNOSIS_CODE LIKE "%U071%"
   OR PRIMARY_DIAGNOSIS_CODE LIKE "%U072%" """)

# COMMAND ----------

#HES_APC - Hospitalisations (COVID in any position)
#Get all patients hospitalised with a covid diagnosis U07.1 or U07.2
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}apc_covid_any_position as
SELECT person_id_deid, date, 
"02_Covid_admission_any_position" as covid_phenotype,
(case when DIAG_4_CONCAT LIKE "%U071%" THEN 'U07.1'
when DIAG_4_CONCAT LIKE "%U072%" THEN 'U07.2' Else '0' End) as clinical_code,
(case when DIAG_4_CONCAT LIKE "%U071%" THEN 'Confirmed_COVID19'
when DIAG_4_CONCAT LIKE "%U072%" THEN 'Suspected_COVID19' Else '0' End) as description,
(case when DIAG_4_CONCAT LIKE "%U071%" THEN 'confirmed'
when DIAG_4_CONCAT LIKE "%U072%" THEN 'suspected' Else '0' End) as covid_status,
"HES APC" as source, 
"ICD10" as code, date_is, SUSRECID
FROM {collab_database_name}.{project_prefix}{temp_hes_apc} """)

# COMMAND ----------

#HES_APC - Hospitalisations (COVID in primary position)
#Get all patients hospitalised with a covid diagnosis U07.1 or U07.2
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}apc_covid_primary_position as
SELECT person_id_deid, date, 
"02_Covid_admission_primary_position" as covid_phenotype,
(case when  DIAG_4_01 LIKE "%U071%" THEN 'U07.1'
when  DIAG_4_01 LIKE "%U072%" THEN 'U07.2' Else '0' End) as clinical_code,
(case when  DIAG_4_01 LIKE "%U071%" THEN 'Confirmed_COVID19'
when  DIAG_4_01 LIKE "%U072%" THEN 'Suspected_COVID19' Else '0' End) as description,
(case when  DIAG_4_01 LIKE "%U071%" THEN 'confirmed'
when  DIAG_4_01 LIKE "%U072%" THEN 'suspected' Else '0' End) as covid_status,
"HES APC" as source, 
"ICD10" as code, date_is, SUSRECID
FROM {collab_database_name}.{project_prefix}{temp_hes_apc} """)
# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: Trajectory table
# COMMAND ----------
spark.sql(F""" drop table if exists {collab_database_name}.{project_prefix}{temp_trajectory}""")
# COMMAND ----------
#Initiate a temporary trajecotry table with the SGSS data
spark.sql(f"""
CREATE TABLE {collab_database_name}.{project_prefix}{temp_trajectory} 
as 
SELECT DISTINCT * FROM global_temp.{project_prefix}sgss_covid """)

# COMMAND ----------
# Append each of the covid related events tables to the temporary trajectory table
for table in ['ccu002_01_gdppr_covid','ccu002_01_apc_covid_any_position','ccu002_01_apc_covid_primary_position','ccu002_01_sus_covid_any_position','ccu002_01_sus_covid_primary_position']:
  spark.sql(f"""REFRESH TABLE global_temp.{table}""")
  (spark.table(f'global_temp.{table}')
   .select("person_id_deid", "date", "covid_phenotype", "clinical_code", "description", "covid_status", "code", "source", "date_is")
   .distinct()
   .write.format("delta").mode('append')
   .saveAsTable(f"{collab_database_name}.{project_prefix}{temp_trajectory}"))


 
