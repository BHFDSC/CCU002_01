# Databricks notebook source
# MAGIC %md # CCU002_01-D07-covariate_flags
# MAGIC  
# MAGIC **Description** Covariate flags across GDPPR, HES.
# MAGIC  
# MAGIC **Author(s)** Based on work by Venexia Walker, Sam Ip & Spencer Keene
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
# MAGIC **Data input** 
# MAGIC 
# MAGIC **Data output**
# MAGIC 
# MAGIC `dars_nic_391419_j3w9t_collab.ccu002_01_included_patients_allcovariates_raw`
# MAGIC 
# MAGIC `dars_nic_391419_j3w9t_collab.ccu002_01_included_patients_allcovariates_final`
# MAGIC 
# MAGIC **Software and versions** SQL, Python
# MAGIC  
# MAGIC **Packages and versions** Not applicable

# COMMAND ----------

%run "/Workspaces/dars_nic_391419_j3w9t_collab/CCU002_01/CCU002_01-functions/covariate_functions"

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Infection parameters

# COMMAND ----------

#Dataset parameters
project_prefix = 'ccu002_01_'
collab_database_name = 'dars_nic_391419_j3w9t_collab'
gdppr_data = 'gdppr_dars_nic_391419_j3w9t'
deaths_data = 'deaths_dars_nic_391419_j3w9t'
sgss_data = 'sgss_dars_nic_391419_j3w9t'
hes_apc_data = 'hes_apc_all_years'
meds_data = 'primary_care_meds_dars_nic_391419_j3w9t'
codelist_table = 'codelist'

#Date parameters
index_date = '2020-01-01'
previous_year_date = '2019-01-01'
previous_3month_date = '2019-10-01'
previous_3months_after_date_string ='2019-09-30'

#Other data inputs
skinny_QA_inclusion_table = 'inf_included_patients'

#Final table name
covariate_flags_final_table = 'included_patients_allcovariates'



# COMMAND ----------

from pyspark.sql.functions import countDistinct, year, dayofmonth, from_unixtime, month, unix_timestamp, to_timestamp, date_format, col, datediff, to_date, lit, months_between
import pandas as pd
#import numpy as np
import numpy as int
import seaborn as sns
import matplotlib.pyplot as plt
import datetime
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql import Window
import io
from functools import reduce
from pyspark.sql.types import StringType


# COMMAND ----------

codelist = spark.table(f"{collab_database_name}.{project_prefix}{codelist_table}")
gdppr = spark.table(collab_database_name + '.' + project_prefix + gdppr_data )
hes_apc = spark.table(collab_database_name + '.' + project_prefix + hes_apc_data)
depriv = spark.table('dss_corporate.english_indices_of_dep_v02')
meds = spark.table(collab_database_name + '.' + project_prefix + meds_data)
region_lookup = spark.table('dars_nic_391419_j3w9t_collab.CCU002_01_lsoa_region_lookup_rk_210708').select("lsoa_code", "region_name")  #note: this is equivalent to dars_nic_391419_j3w9t_collab.curr901a_lsoa_region_lookup
# depriv = spark.table('dss_corporate.eng_indices_of_dep_quantiles')
#common_cutoff_date = to_date(lit(index_date))

# COMMAND ----------

 #MAGIC %md
 #### Long format HES APC

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_01_hes_apc_longformat AS
 SELECT *, LEFT ( REGEXP_REPLACE(CODE,'[.,-,' ',X$]','') , 4 ) AS code_trunc
 FROM (SELECT PERSON_ID_DEID AS NHS_NUMBER_DEID, 
              ADMIDATE, 
              DISDATE,
              EPISTART, 
              EPIEND,
              EPIKEY,
              STACK(40,
                    DIAG_3_01, 'DIAG_3_01',
                    DIAG_3_02, 'DIAG_3_02',
                    DIAG_3_03, 'DIAG_3_03',
                    DIAG_3_04, 'DIAG_3_04',
                    DIAG_3_05, 'DIAG_3_05',
                    DIAG_3_06, 'DIAG_3_06',
                    DIAG_3_07, 'DIAG_3_07',
                    DIAG_3_08, 'DIAG_3_08',
                    DIAG_3_09, 'DIAG_3_09',
                    DIAG_3_10, 'DIAG_3_10',
                    DIAG_3_11, 'DIAG_3_11',
                    DIAG_3_12, 'DIAG_3_12',
                    DIAG_3_13, 'DIAG_3_13',
                    DIAG_3_14, 'DIAG_3_14',
                    DIAG_3_15, 'DIAG_3_15',
                    DIAG_3_16, 'DIAG_3_16',
                    DIAG_3_17, 'DIAG_3_17',
                    DIAG_3_18, 'DIAG_3_18',
                    DIAG_3_19, 'DIAG_3_19',
                    DIAG_3_20, 'DIAG_3_20',
                    DIAG_4_01, 'DIAG_4_01',
                    DIAG_4_02, 'DIAG_4_02',
                    DIAG_4_03, 'DIAG_4_03',
                    DIAG_4_04, 'DIAG_4_04',
                    DIAG_4_05, 'DIAG_4_05',
                    DIAG_4_06, 'DIAG_4_06',
                    DIAG_4_07, 'DIAG_4_07',
                    DIAG_4_08, 'DIAG_4_08',
                    DIAG_4_09, 'DIAG_4_09',
                    DIAG_4_10, 'DIAG_4_10',
                    DIAG_4_11, 'DIAG_4_11',
                    DIAG_4_12, 'DIAG_4_12',
                    DIAG_4_13, 'DIAG_4_13',
                    DIAG_4_14, 'DIAG_4_14',
                    DIAG_4_15, 'DIAG_4_15',
                    DIAG_4_16, 'DIAG_4_16',
                    DIAG_4_17, 'DIAG_4_17',
                    DIAG_4_18, 'DIAG_4_18',
                    DIAG_4_19, 'DIAG_4_19',
                    DIAG_4_20, 'DIAG_4_20') AS (CODE, SOURCE)
       FROM dars_nic_391419_j3w9t_collab.ccu002_01_hes_apc_all_years)
 WHERE NHS_NUMBER_DEID IS NOT NULL AND CODE IS NOT NULL

# COMMAND ----------

 #MAGIC %md
 #### Covariate codelist

# COMMAND ----------

 %sql
 ----All relevent covariate codelists
 ----Some covaraite names have _covariate_only on the end so removing for ease
 
 create or replace global temp view CCU002_01_covariate_codelist as
 
 SELECT name, terminology, code,
 CASE
 WHEN name LIKE 'depression%' THEN 'depression' 
 WHEN name LIKE 'BMI_obesity%' THEN 'BMI_obesity'
 WHEN name LIKE 'COPD%' THEN 'COPD'
 WHEN name LIKE 'hypertension%' THEN 'hypertension'
 WHEN name LIKE 'diabetes%' THEN 'diabetes'
 WHEN name LIKE 'cancer%' THEN 'cancer'
 WHEN name LIKE 'liver_disease%' THEN 'liver_disease'
 WHEN name LIKE 'dementia%' THEN 'dementia'
 WHEN name LIKE 'CKD%' THEN 'CKD'
 WHEN name LIKE 'AMI%' THEN 'AMI'
 WHEN name LIKE 'DVT_DVT%' OR name LIKE 'other_DVT%' OR name LIKE 'DVT_pregnancy%' OR name LIKE 'ICVT_pregnancy%' OR name LIKE 'portal_vein_thrombosis%' OR name LIKE 'VT_covariate_only' THEN 'VT'
 WHEN name LIKE 'DVT_ICVT%' THEN 'DVT_ICVT'
 WHEN name LIKE 'PE%' THEN 'PE' 
 WHEN name LIKE 'stroke_isch%' THEN 'stroke_isch'
 WHEN name LIKE 'stroke_SAH_HS%' THEN 'stroke_SAH_HS'
 WHEN name LIKE 'thrombophilia%' THEN 'thrombophilia'
 WHEN name LIKE 'thrombocytopenia%' OR name LIKE 'TTP%' OR name LIKE 'TCP_covariate_only%' THEN 'TCP'
 WHEN name LIKE 'angina%' OR name LIKE 'unstable_angina%' THEN 'angina'
 WHEN name LIKE 'other_arterial_embolism%' THEN 'other_arterial_embolism'
 WHEN name LIKE 'DIC%' THEN 'DIC'
 WHEN name LIKE 'mesenteric_thrombus%' THEN 'mesenteric_thrombus'
 WHEN name LIKE 'artery_dissect%' THEN 'artery_dissect'
 WHEN name LIKE 'life_arrhythmia%' THEN 'life_arrhythmia'
 WHEN name LIKE 'cardiomyopathy%' THEN 'cardiomyopathy'
 WHEN name LIKE 'HF%' THEN 'HF'
 WHEN name LIKE 'pericarditis%' THEN 'pericarditis'
 WHEN name LIKE 'myocarditis%' THEN 'myocarditis'
 WHEN name LIKE 'stroke_TIA%' THEN 'stroke_TIA'
 else name
 END AS covariate_name,
 
 CASE 
 WHEN terminology = 'ICD10' THEN LEFT ( REGEXP_REPLACE(code,'[.,-,' ']','') , 4 )
 ELSE code
 END AS code_trunc
 
 FROM dars_nic_391419_j3w9t_collab.ccu002_01_codelist
 WHERE (name LIKE 'depression%' OR name LIKE 'BMI_obesity%' OR name LIKE 'COPD%' OR name LIKE 'hypertension%' OR name LIKE 'diabetes%' OR name LIKE 'cancer%' OR name LIKE 'liver_disease%' OR name LIKE 'dementia%' OR name LIKE 'CKD%' OR name LIKE 'AMI%' OR name LIKE 'DVT_DVT%' OR name LIKE 'other_DVT%' OR name LIKE 'DVT_pregnancy%' OR name LIKE 'ICVT_pregnancy%' OR name LIKE 'portal_vein_thrombosis%' OR name LIKE 'VT_covariate_only' OR name LIKE 'DVT_ICVT%' OR name LIKE 'PE%' OR name LIKE 'stroke_isch%' OR name LIKE 'stroke_SAH_HS%' OR name LIKE 'thrombophilia%' OR name LIKE 'thrombocytopenia%' OR name LIKE 'TTP%' OR name LIKE 'TCP_covariate_only%' OR name LIKE 'angina%' OR name LIKE 'unstable_angina%' OR name LIKE 'other_arterial_embolism%' OR name LIKE 'DIC%' OR name LIKE 'mesenteric_thrombus%' OR name LIKE 'artery_dissect%' OR name LIKE 'life_arrhythmia%' OR name LIKE 'cardiomyopathy%' OR name LIKE 'HF%' OR name LIKE 'pericarditis%' OR name LIKE 'myocarditis%' OR name LIKE 'stroke_TIA%' OR name LIKE 'antiplatelet' OR name LIKE 'bp_lowering' OR name LIKE 'lipid_lowering' OR name LIKE 'anticoagulant' OR name LIKE 'cocp' OR name LIKE 'hrt')

# COMMAND ----------

 #MAGIC %md 
 #### Primary care medical history

# COMMAND ----------

snomed = spark.sql("SELECT DISTINCT covariate_name FROM global_temp.CCU002_01_covariate_codelist WHERE terminology=='SNOMED' or terminology=='CTV3_SNOMEDmapped'")
snomed = [item.covariate_name for item in snomed.select('covariate_name').collect()]

# COMMAND ----------

for covariate in snomed:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_01_cov_snomed_" + covariate + " AS SELECT distinct NHS_NUMBER_DEID, 1 AS " + covariate + " FROM dars_nic_391419_j3w9t_collab.ccu002_01_gdppr_dars_nic_391419_j3w9t WHERE CODE IN (SELECT code_trunc FROM global_temp.CCU002_01_covariate_codelist WHERE covariate_name like '" + covariate + "' AND (TERMINOLOGY='SNOMED' or terminology=='CTV3_SNOMEDmapped')) AND DATE < '2020-01-01' ")

# COMMAND ----------

 #MAGIC %md 
 #### Secondary care medical history

# COMMAND ----------

icd10 = spark.sql("SELECT DISTINCT covariate_name FROM global_temp.CCU002_01_covariate_codelist WHERE terminology=='ICD10' ")
icd10 = [item.covariate_name for item in icd10.select('covariate_name').collect()]

# COMMAND ----------

for covariate in icd10:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_01_cov_icd10_" + covariate + " AS SELECT distinct NHS_NUMBER_DEID, 1 AS " + covariate + " FROM dars_nic_391419_j3w9t_collab.ccu002_01_hes_apc_longformat WHERE code_trunc IN (SELECT code_trunc FROM global_temp.CCU002_01_covariate_codelist WHERE covariate_name like '" + covariate + "' AND TERMINOLOGY='ICD10') AND EPISTART < '2020-01-01' ")

# COMMAND ----------

 #MAGIC %md 
 #### Primary care medication history

# COMMAND ----------

dmd = spark.sql("SELECT DISTINCT covariate_name FROM global_temp.CCU002_01_covariate_codelist WHERE terminology=='DMD'")
dmd = [item.covariate_name for item in dmd.select('covariate_name').collect()]

# COMMAND ----------

for covariate in dmd:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_01_cov_dmd_" + covariate + " AS SELECT distinct NHS_NUMBER_DEID, 1 AS " + covariate + " FROM dars_nic_391419_j3w9t_collab.ccu002_01_gdppr_dars_nic_391419_j3w9t WHERE CODE IN (SELECT code_trunc FROM global_temp.CCU002_01_covariate_codelist WHERE covariate_name like '" + covariate + "' AND TERMINOLOGY=='DMD') AND (DATE < '2020-01-01' AND DATE > '2019-09-30') ")

# COMMAND ----------

 #MAGIC %md
 #### Combine all medical historis for each covariate

# COMMAND ----------

 %sql
 create or replace global temp view ccu002_01_covariate_terminology as
 
 with cte_terminology as (
 select distinct covariate_name, terminology
 from global_temp.CCU002_01_covariate_codelist
 order by covariate_name),
 
 cte_which_terminology as (
 select distinct covariate_name,
 case 
 when terminology = 'SNOMED' then 1 else 0 
 end as snomed,
 
 case 
 when terminology = 'ICD10' then 1 else 0
 end as icd10,
 
 case 
 when terminology = 'DMD' then 1 else 0
 end as DMD
 from cte_terminology)
 
 select covariate_name, sum(snomed) as snomed, sum(icd10) as icd10, sum(dmd) as dmd
 from cte_which_terminology
 group by covariate_name

# COMMAND ----------

dmd_icd10_snomed = spark.sql("SELECT DISTINCT covariate_name FROM global_temp.CCU002_01_covariate_terminology WHERE snomed=='1' and icd10='1' and dmd='1'")
dmd_icd10_snomed = [item.covariate_name for item in dmd_icd10_snomed.select('covariate_name').collect()]

icd10_snomed = spark.sql("SELECT DISTINCT covariate_name FROM global_temp.CCU002_01_covariate_terminology WHERE snomed=='1' and icd10='1' and dmd='0'")
icd10_snomed = [item.covariate_name for item in icd10_snomed.select('covariate_name').collect()]

icd10_only = spark.sql("SELECT DISTINCT covariate_name FROM global_temp.CCU002_01_covariate_terminology WHERE snomed=='0' and icd10='1' and dmd='0'")
icd10_only = [item.covariate_name for item in icd10_only.select('covariate_name').collect()]

snomed_only = spark.sql("SELECT DISTINCT covariate_name FROM global_temp.CCU002_01_covariate_terminology WHERE snomed=='1' and icd10='0' and dmd='0'")
snomed_only = [item.covariate_name for item in snomed_only.select('covariate_name').collect()]

# COMMAND ----------

for covariate in dmd_icd10_snomed:
  sql("create table dars_nic_391419_j3w9t_collab.ccu002_01_temp_cov_" + covariate + " as with cte_combined as (select nhs_number_deid from global_temp.ccu002_01_cov_snomed_" + covariate + " union all select nhs_number_deid from global_temp.ccu002_01_cov_icd10_" + covariate + " union all select nhs_number_deid from global_temp.ccu002_01_cov_dmd_" + covariate + ") select distinct nhs_number_deid, 1 AS " + covariate + " from cte_combined")

# COMMAND ----------

for covariate in dmd_icd10_snomed:
  sql(" ALTER TABLE dars_nic_391419_j3w9t_collab.ccu002_01_temp_cov_" + covariate + " OWNER TO dars_nic_391419_j3w9t_collab ")

# COMMAND ----------

for covariate in icd10_snomed:
  sql("create table dars_nic_391419_j3w9t_collab.ccu002_01_temp_cov_" + covariate + " as with cte_combined as (select nhs_number_deid from global_temp.ccu002_01_cov_snomed_" + covariate + " union all select nhs_number_deid from global_temp.ccu002_01_cov_icd10_" + covariate + ") select distinct nhs_number_deid, 1 AS " + covariate + " from cte_combined")

# COMMAND ----------

for covariate in icd10_snomed:
  sql(" ALTER TABLE dars_nic_391419_j3w9t_collab.ccu002_01_temp_cov_" + covariate + " OWNER TO dars_nic_391419_j3w9t_collab ")

# COMMAND ----------

for covariate in icd10_only:
  sql("create table dars_nic_391419_j3w9t_collab.ccu002_01_temp_cov_" + covariate + " as select * from global_temp.ccu002_01_cov_icd10_" + covariate + " ")

# COMMAND ----------

for covariate in icd10_only:
  sql(" ALTER TABLE dars_nic_391419_j3w9t_collab.ccu002_01_temp_cov_" + covariate + " OWNER TO dars_nic_391419_j3w9t_collab ")

# COMMAND ----------

for covariate in snomed_only:
  sql("create table dars_nic_391419_j3w9t_collab.ccu002_01_temp_cov_" + covariate + " as select * from global_temp.ccu002_01_cov_snomed_" + covariate + " ")


# COMMAND ----------

 #MAGIC %md 
 #### Medication history

# COMMAND ----------

bnf = spark.sql("SELECT DISTINCT covariate_name FROM global_temp.CCU002_01_covariate_codelist WHERE terminology=='BNF'")
bnf = [item.covariate_name for item in bnf.select('covariate_name').collect()]

# COMMAND ----------

for covariate in bnf:
   sql("CREATE TABLE  dars_nic_391419_j3w9t_collab.ccu002_01_temp_cov_" + covariate + " AS SELECT distinct person_id_deid , 1 AS " + covariate + " FROM dars_nic_391419_j3w9t_collab.ccu002_01_primary_care_meds_dars_nic_391419_j3w9t WHERE PrescribedBNFCode IN (SELECT code_trunc FROM global_temp.CCU002_01_covariate_codelist WHERE covariate_name like '" + covariate + "' AND TERMINOLOGY=='BNF') AND (ProcessingPeriodDate < '2020-01-01' AND ProcessingPeriodDate >= '2019-10-01') ")

# COMMAND ----------

 #MAGIC %md
 #### Unique BNF chaps

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_01_cov_unique_bnf_chapters AS
 with cte_BNFChapter as (
 select PERSON_ID_DEID, left(PrescribedBNFCode, 2) as BNFChapter, ProcessingPeriodDate
 from dars_nic_391419_j3w9t_collab.ccu002_01_primary_care_meds_dars_nic_391419_j3w9t
 )
 
 SELECT PERSON_ID_DEID, COUNT(DISTINCT BNFChapter) AS unique_bnf_chapters
 FROM cte_BNFChapter
 WHERE ProcessingPeriodDate < '2020-01-01' AND ProcessingPeriodDate >= '2019-10-01'
 GROUP BY PERSON_ID_DEID

# COMMAND ----------

 #MAGIC %md
 #### Addtional obesity BMI data

# COMMAND ----------

#Save as global temp table
#Create a table with the latest BMI record for someone that is not null prior to 
#The snomed codes were dervied by doing a string search and selecting the most relevant codes (this was in an R script) 


spark.sql(F"""create or replace global temp view {project_prefix}bmi_covariate as

with cte as 
(select * from

(select *, ROW_NUMBER() OVER (PARTITION BY nhs_number_deid order by date desc) as RN --date as opposed to record_date, more complete
from {collab_database_name}.{project_prefix}{gdppr_data}
where code rlike '722595002|914741000000103|914731000000107|914721000000105|35425004|48499001|301331008|6497000|310252000|427090001|408512008|162864005|162863004|412768003|60621009|846931000000101'
and date < '{index_date}' AND nhs_number_deid is not null 
and value1_condition is not null
and (value1_condition<=100 and value1_condition >=12) --Biological plausible limits
)
where RN ==1)

select nhs_number_deid as ID, value1_condition as BMI,
case when value1_condition>=30 then 1 else 0 end as OBESE_BMI
from cte""")



# COMMAND ----------

 #MAGIC %md 
 #### Deprivation and regions

# COMMAND ----------

map = depriv.filter(depriv["IMD_YEAR"] == 2019).select(["LSOA_CODE_2011", "DECI_IMD", "IMD_YEAR"]).drop("IMD_YEAR").withColumnRenamed('LSOA_CODE_2011', 'LSOA')
#display(map)

# COMMAND ----------

df = gdppr.select(["LSOA", "NHS_NUMBER_DEID", "RECORD_DATE"])
df = df.filter(col("RECORD_DATE") < index_date) #.filter(col("RECORD_DATE") >= "2019-01-01")
df = df.filter(df["LSOA"].isNotNull()).dropDuplicates()
# print(df.select("NHS_NUMBER_DEID").distinct().count())
#display(df)

# COMMAND ----------

# get latest RECORD_DATE per PAT
w = Window.partitionBy('NHS_NUMBER_DEID').orderBy(desc('RECORD_DATE'))
df_latestrec = df.withColumn('RANK_LATEST_RECDATE',dense_rank().over(w))
df_latestrec = df_latestrec.filter(df_latestrec.RANK_LATEST_RECDATE == 1)
## .drop(df_latestrec.RANK_LATEST_RECDATE)
#display(df_latestrec)

# COMMAND ----------

# those who had well-defined latest pre-2020 LSOA
df_lsoa = df_latestrec.groupBy("NHS_NUMBER_DEID").count().where("count == 1").withColumnRenamed('count', 'N_LATESTREC')
df_lsoa = df_lsoa.join(df_latestrec, "NHS_NUMBER_DEID", "left")
#display(df_lsoa)

# COMMAND ----------

# who has multiple entries?
df_clash = df_latestrec.groupBy("NHS_NUMBER_DEID").count().where("count > 1").withColumnRenamed('count', 'N_LATESTREC')
# .select("NHS_NUMBER_DEID", "N_LATESTREC")
df_clash = df_clash.join(df, "NHS_NUMBER_DEID", "left")
#display(df_clash)

# COMMAND ----------

# use MODE LSOA over past yr for clashes -- previously (@df) already removed duplicated ID | LSOA | RECORD_DATE and restricted to past yr
df_mostfreq_lsoa = df_clash.groupBy('NHS_NUMBER_DEID', 'LSOA').count().withColumnRenamed('count', 'FREQ_LSOA_UNQIUE_PASTYR_DATES')
# rank them in terms of freq -- pick mode
w = Window.partitionBy("NHS_NUMBER_DEID").orderBy(desc("FREQ_LSOA_UNQIUE_PASTYR_DATES"))
df_mostfreq_lsoa = df_mostfreq_lsoa.withColumn('MOST_FREQ_LSOA', row_number().over(w)).where(col('MOST_FREQ_LSOA') == 1)
#display(df_mostfreq_lsoa)

# COMMAND ----------

df_uniqlsoa_default = df_lsoa.select("NHS_NUMBER_DEID", "LSOA")
df_uniqlsoa_clash = df_mostfreq_lsoa.select("NHS_NUMBER_DEID", "LSOA")
# concat df_clash to df_lsoa
df_lsoa_final = df_uniqlsoa_default.union(df_uniqlsoa_clash)
#display(df_lsoa_final)

# COMMAND ----------

# map LSOAs to IMD
df_imd = df_lsoa_final.join(map, "LSOA", "left")
df_imd = df_imd.join(region_lookup, df_imd.LSOA == region_lookup.lsoa_code, how='left')
df_imd= df_imd.dropDuplicates()
df_imd = df_imd.withColumnRenamed('NHS_NUMBER_DEID', 'ID')
df_imd.createOrReplaceGlobalTempView(f" {project_prefix}cov_imd")

# COMMAND ----------

 #MAGIC %md
 #### Surgery last year

# COMMAND ----------

# codelist
surgery_opertn_codelist = [chr(i) for i in range(ord('A'),ord('U'))] + [chr(i) for i in range(ord('V'),ord('X'))]

# COMMAND ----------

# ["%,X%", "X%"]
surgery_opertn_codelist_firstposition = ['%,'+ code + '%' for code in surgery_opertn_codelist] + [code + '%' for code in surgery_opertn_codelist]

# COMMAND ----------

hes_ever = hes_apc.withColumnRenamed('PERSON_ID_DEID', 'ID').select(['ID', "EPISTART", "OPERTN_3_CONCAT"]).filter(col("EPISTART") < index_date).filter(col("EPISTART") >= previous_year_date)
df_surgery = hes_ever.where(
#     reduce(lambda a, b: a|b, (hes_lastyr['OPERTN_3_CONCAT'].like('%'+code+"%") for code in surgery_opertn_codelist))
    reduce(lambda a, b: a|b, (hes_ever['OPERTN_3_CONCAT'].like(code) for code in surgery_opertn_codelist_firstposition))  
).select(["ID", "EPISTART", "OPERTN_3_CONCAT"]).withColumn("SURGERY_LASTYR_HES", lit(1))

#  checked that this includes patient with OPERTN_3_CONCAT = X70,L99


# COMMAND ----------

df_surgery= df_surgery.dropDuplicates()
df_surgery = df_surgery.select("ID", "SURGERY_LASTYR_HES")
df_surgery= df_surgery.dropDuplicates()
df_surgery.createOrReplaceGlobalTempView(f"{project_prefix}cov_surgery")

# COMMAND ----------

 #MAGIC %md
 #### Smoking status

# COMMAND ----------

spark.sql(f"""
create or replace global temp view {project_prefix}smokingstatus_SNOMED as
select *
from {collab_database_name}.{project_prefix}{codelist_table}
where name in ('Current-smoker','Never-smoker','Ex-smoker')
""")

# COMMAND ----------

spark.sql(F"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}covariates_smoking as

SELECT tab2.NHS_NUMBER_DEID, tab2.DATE, tab2.code, tab1.name, tab1.terminology, tab1.term
FROM  global_temp.{project_prefix}smokingstatus_SNOMED tab1
inner join {collab_database_name}.{project_prefix}{gdppr_data} tab2 on tab1.code= tab2.code
WHERE DATE < '{index_date}'""")



# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}covariates_smoking_final as

with cte_latest as ( 
select * from (
    select NHS_NUMBER_DEID as ID, DATE, code, term, name,
        row_number() over(partition by NHS_NUMBER_DEID order by DATE desc) as rn
    from
        global_temp.{project_prefix}covariates_smoking
) t
where t.rn = 1
)

select ID, name
FROM cte_latest 
""")

# COMMAND ----------

#the most recent record
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}covariates_smoking_final as

with cte_latest as ( 
select * from (
    select NHS_NUMBER_DEID as ID, DATE, code, term, name,
        row_number() over(partition by NHS_NUMBER_DEID order by DATE desc) as rn
    from
        global_temp.{project_prefix}covariates_smoking
) t
where t.rn = 1
)

select ID, name as smoking_status
FROM cte_latest """) 


# COMMAND ----------

df_smoking = spark.table(f"global_temp.{project_prefix}covariates_smoking_final")
df_smoking = df_smoking.dropDuplicates()
df_smoking.createOrReplaceGlobalTempView(f"{project_prefix}cov_smoking_final")

# COMMAND ----------

 #MAGIC %md
 #### Number of disorders

# COMMAND ----------

df1 = gdppr.select(["LSOA", "NHS_NUMBER_DEID", "RECORD_DATE", "CODE"]).filter(gdppr["RECORD_DATE"].isNotNull())


# COMMAND ----------

#Lookup obtained from running notebook in functions folder called 'get_children'

df2= spark.sql("select * from dars_nic_391419_j3w9t_collab.ccu002_vac_disorder_lookup_full") 

df2=df2.select("CONCEPT_ID")

# COMMAND ----------

df = df1.join(df2, df1['code']==df2['CONCEPT_ID'], how='inner')
#df.show()

# COMMAND ----------

df = df.filter(df.RECORD_DATE >= to_date(lit(previous_year_date))).filter(df.RECORD_DATE < to_date(lit(index_date))).select("NHS_NUMBER_DEID", "RECORD_DATE", "CONCEPT_ID").dropDuplicates()


# COMMAND ----------

df_disorder = df.groupBy("NHS_NUMBER_DEID").agg(f.countDistinct("CONCEPT_ID").alias("N_DISORDER"))


#This could either be count distinct record_date or count distinct concept_id, Ive gone with conceptID because googledoc says number of unique codes over the last year (rather than consultations)

# COMMAND ----------

df_disorder= df_disorder.dropDuplicates()
df_disorder = df_disorder.withColumnRenamed('NHS_NUMBER_DEID', 'ID')
df_disorder.createOrReplaceGlobalTempView(f"{project_prefix}cov_disorders")

# COMMAND ----------

 #MAGIC %md 
 ### Make covariates table

# COMMAND ----------

spark.sql(F"""
create or replace global temp view {project_prefix}skinny_all_covariates as

with cte_cov as (
SELECT skinny.NHS_NUMBER_DEID, skinny.CATEGORISED_ETHNICITY, skinny.SEX, skinny.DATE_OF_BIRTH, skinny.DATE_OF_DEATH, skinny.AGE_AT_COHORT_START, 
imd.LSOA, imd.DECI_IMD, imd.lsoa_code, imd.region_name,
--code.UNIQUE_MED_CODES,
bnf.unique_bnf_chapters,
antipl.antiplatelet,
bpl.bp_lowering,
lipidl.lipid_lowering,
anticoag.anticoagulant,
cocp.cocp,
hrt.hrt,
depr.depression,
obese1.BMI_obesity, 
obese2.OBESE_BMI, --obese2.BMI,
copd.COPD,
surg.SURGERY_LASTYR_HES,
ht.hypertension,
diab.diabetes,
can.cancer,
liver.liver_disease,
demen.dementia,
ckd.CKD,
ami.AMI,
vt.VT,
icvt.DVT_ICVT,
pe.PE,
ischstroke.stroke_isch,
stroke_sah_hs.stroke_SAH_HS,
thrombo.thrombophilia,
tcp.TCP,
smok.smoking_status,
disorders.N_DISORDER,
angina.angina,
arterial_embolism.other_arterial_embolism,
DIC.DIC,
mesenteric_thrombus.mesenteric_thrombus,
artery_dissect.artery_dissect,
life_arrhythmia.life_arrhythmia,
cardiomyopathy.cardiomyopathy,
HF.HF,
pericarditis.pericarditis,
myocarditis.myocarditis,
stroke_TIA.stroke_TIA
--disorders.consult_groups,
--covid.COVID_infection,

FROM {collab_database_name}.{project_prefix}{skinny_QA_inclusion_table} as skinny
LEFT JOIN global_temp.{project_prefix}cov_imd as imd
ON skinny.NHS_NUMBER_DEID = imd.ID
--LEFT JOIN global_temp.{project_prefix}covariates_med_uniquecode as code
--ON skinny.NHS_NUMBER_DEID = code.ID
LEFT JOIN global_temp.{project_prefix}cov_unique_bnf_chapters as bnf
ON skinny.NHS_NUMBER_DEID = bnf.person_id_deid
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_antiplatelet as antipl
ON skinny.NHS_NUMBER_DEID = antipl.person_id_deid
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_bp_lowering as bpl
ON skinny.NHS_NUMBER_DEID = bpl.person_id_deid
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_lipid_lowering as lipidl
ON skinny.NHS_NUMBER_DEID = lipidl.person_id_deid 
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_anticoagulant as anticoag
ON skinny.NHS_NUMBER_DEID = anticoag.person_id_deid
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_cocp as cocp
ON skinny.NHS_NUMBER_DEID = cocp.person_id_deid 
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_hrt as hrt
ON skinny.NHS_NUMBER_DEID = hrt.person_id_deid 
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_depression as depr
ON skinny.NHS_NUMBER_DEID = depr.NHS_NUMBER_DEID
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_BMI_obesity as obese1 
ON skinny.NHS_NUMBER_DEID = obese1.NHS_NUMBER_DEID
LEFT JOIN global_temp.{project_prefix}bmi_covariate as obese2 
ON skinny.NHS_NUMBER_DEID = obese2.ID
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_COPD as copd
ON skinny.NHS_NUMBER_DEID = copd.NHS_NUMBER_DEID
LEFT JOIN global_temp.{project_prefix}cov_surgery as surg
ON skinny.NHS_NUMBER_DEID = surg.ID
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_hypertension as ht
ON skinny.NHS_NUMBER_DEID = ht.NHS_NUMBER_DEID
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_diabetes as diab
ON skinny.NHS_NUMBER_DEID = diab.NHS_NUMBER_DEID
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_cancer as can
ON skinny.NHS_NUMBER_DEID = can.NHS_NUMBER_DEID
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_liver_disease as liver
ON skinny.NHS_NUMBER_DEID = liver.NHS_NUMBER_DEID
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_dementia as demen
ON skinny.NHS_NUMBER_DEID = demen.NHS_NUMBER_DEID
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_CKD as ckd
ON skinny.NHS_NUMBER_DEID = ckd.NHS_NUMBER_DEID
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_AMI as ami
ON skinny.NHS_NUMBER_DEID = ami.NHS_NUMBER_DEID
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_VT as vt
ON skinny.NHS_NUMBER_DEID = vt.NHS_NUMBER_DEID
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_DVT_ICVT as icvt
ON skinny.NHS_NUMBER_DEID = icvt.NHS_NUMBER_DEID
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_PE as pe
ON skinny.NHS_NUMBER_DEID = pe.NHS_NUMBER_DEID
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_stroke_isch as ischstroke
ON skinny.NHS_NUMBER_DEID = ischstroke.NHS_NUMBER_DEID
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_stroke_SAH_HS as stroke_sah_hs
ON skinny.NHS_NUMBER_DEID = stroke_sah_hs.NHS_NUMBER_DEID
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_thrombophilia as thrombo
ON skinny.NHS_NUMBER_DEID = thrombo.NHS_NUMBER_DEID
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_TCP as tcp
ON skinny.NHS_NUMBER_DEID = tcp.NHS_NUMBER_DEID
LEFT JOIN global_temp.{project_prefix}covariates_smoking_final as smok
ON skinny.NHS_NUMBER_DEID = smok.ID
LEFT JOIN global_temp.{project_prefix}cov_disorders as disorders
ON skinny.NHS_NUMBER_DEID = disorders.ID
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_angina as angina
ON skinny.NHS_NUMBER_DEID = angina.NHS_NUMBER_DEID
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_other_arterial_embolism as arterial_embolism
ON skinny.NHS_NUMBER_DEID = arterial_embolism.NHS_NUMBER_DEID
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_DIC as DIC
ON skinny.NHS_NUMBER_DEID = DIC.NHS_NUMBER_DEID
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_mesenteric_thrombus as mesenteric_thrombus
ON skinny.NHS_NUMBER_DEID = mesenteric_thrombus.NHS_NUMBER_DEID
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_artery_dissect as artery_dissect
ON skinny.NHS_NUMBER_DEID = artery_dissect.NHS_NUMBER_DEID
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_life_arrhythmia as life_arrhythmia
ON skinny.NHS_NUMBER_DEID = life_arrhythmia.NHS_NUMBER_DEID 
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_cardiomyopathy as cardiomyopathy
ON skinny.NHS_NUMBER_DEID = cardiomyopathy.NHS_NUMBER_DEID
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_HF as HF
ON skinny.NHS_NUMBER_DEID = HF.NHS_NUMBER_DEID
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_pericarditis as pericarditis
ON skinny.NHS_NUMBER_DEID = pericarditis.NHS_NUMBER_DEID 
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_myocarditis as myocarditis
ON skinny.NHS_NUMBER_DEID = myocarditis.NHS_NUMBER_DEID
LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}temp_cov_stroke_TIA as stroke_TIA 
ON skinny.NHS_NUMBER_DEID = stroke_TIA.NHS_NUMBER_DEID

)


select NHS_NUMBER_DEID, CATEGORISED_ETHNICITY, SEX, DATE_OF_BIRTH, DATE_OF_DEATH, AGE_AT_COHORT_START, LSOA, DECI_IMD, region_name, /*UNIQUE_MED_CODES,*/ unique_bnf_chapters, BMI_obesity, OBESE_BMI, COPD, SURGERY_LASTYR_HES, hypertension, diabetes, cancer, liver_disease, dementia, CKD, AMI, VT, DVT_ICVT, PE, stroke_isch, stroke_SAH_HS, thrombophilia, TCP, smoking_status, N_DISORDER, angina, other_arterial_embolism, DIC, mesenteric_thrombus, artery_dissect, life_arrhythmia, cardiomyopathy, HF, pericarditis, myocarditis, stroke_TIA, antiplatelet, bp_lowering, lipid_lowering, anticoagulant, cocp, hrt, depression

from cte_cov
""")



