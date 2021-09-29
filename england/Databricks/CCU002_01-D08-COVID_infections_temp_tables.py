# Databricks notebook source
# MAGIC %md
# MAGIC ## CCU002_01-D08-COVID_infections_temp_tables
# MAGIC  
# MAGIC **Description** 
# MAGIC   
# MAGIC This notebook creates tables for each of the main datasets required to create the COVID infections table. This is based off work from CCU013.
# MAGIC 
# MAGIC **Project(s)** CCU002_01 
# MAGIC  
# MAGIC **Author(s)** Chris Tomlinson, Johan Thygesen (inspired by Sam Hollings)
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
# MAGIC These are frozen datasets which were frozen in the notebook `CCU002_01-D00-project_table_freeze`
# MAGIC * `dars_nic_391419_j3w9t_collab.ccu002_01_chess_dars_nic_391419_j3w9t`
# MAGIC * `dars_nic_391419_j3w9t_collab.ccu002_01_deaths_dars_nic_391419_j3w9t`
# MAGIC * `dars_nic_391419_j3w9t_collab.ccu002_01_gdppr_dars_nic_391419_j3w9t`
# MAGIC * `dars_nic_391419_j3w9t_collab.ccu002_01_sgss_dars_nic_391419_j3w9t`
# MAGIC * `dars_nic_391419_j3w9t_collab.ccu002_01_sus_dars_nic_391419_j3w9t`
# MAGIC * `dars_nic_391419_j3w9t_collab.ccu002_01_hes_apc_all_years`
# MAGIC * `dars_nic_391419_j3w9t_collab.ccu002_01_hes_op_all_years`
# MAGIC * `dars_nic_391419_j3w9t_collab.ccu002_01_hes_ae_all_years`
# MAGIC * `dars_nic_391419_j3w9t_collab.ccu002_01_hes_cc_all_years`
# MAGIC   
# MAGIC   
# MAGIC **Data output**
# MAGIC * `ccu002_1_tmp_sgss`
# MAGIC * `ccu002_01_tmp_gdppr`
# MAGIC * `ccu002_01_tmp_deaths`
# MAGIC * `ccu002_01_tmp_sus`
# MAGIC * `ccu002_01_tmp_apc`
# MAGIC * `ccu002_01_tmp_cc`
# MAGIC * `ccu002_01_tmp_chess`
# MAGIC   
# MAGIC  
# MAGIC **Software and versions** SQl, Python
# MAGIC  
# MAGIC **Packages and versions** `pyspark`

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

#Date parameters
index_date = '2020-01-01' 

#Output
temp_sgss = 'tmp_sgss'
temp_gdppr = 'tmp_gdppr'
temp_deaths = 'tmp_deaths'
temp_hes_apc = 'tmp_hes_apc'
temp_hes_cc = 'tmp_hes_cc'
temp_chess = 'tmp_chess'
temp_sus = 'tmp_sus'

# COMMAND ----------

from pyspark.sql.functions import lit, to_date, col, udf, substring
from datetime import datetime, date #Jenny added date
from pyspark.sql.types import DateType


today = date.today()
end_date = today.strftime('%Y-%m-%d') 

# COMMAND ----------

%run /Workspaces/dars_nic_391419_j3w9t_collab/CCU002_01/CCU002_01-functions/CCU002_01_COVID_trajectory_helper_functions_from_CCU013

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.0 Subseting all source tables by dates

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 SGSS

# COMMAND ----------

# SGSS
sgss = spark.sql(f"""SELECT person_id_deid, REPORTING_LAB_ID, specimen_date FROM {collab_database_name}.{project_prefix}{sgss_data}""")
sgss = sgss.withColumnRenamed('specimen_date', 'date')
sgss = sgss.withColumn('date_is', lit('specimen_date'))
sgss = sgss.filter((sgss['date'] >= index_date) & (sgss['date'] <= end_date))
sgss = sgss.filter(sgss['person_id_deid'].isNotNull())
sgss.createOrReplaceGlobalTempView(f"{project_prefix}tmp_sgss")
#drop_table(collab_database_name +"." + project_prefix + temp_sgss + temp_current_initials_date, if_exists=True)
create_table(project_prefix + temp_sgss, select_sql_script=f"SELECT * FROM global_temp.{project_prefix}tmp_sgss") 
#sgss = sgss.orderBy('specimen_date', ascending = False)
#display(sgss)
#print(sgss.count(), len(sgss.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 GDPPR

# COMMAND ----------

# GDPPR
gdppr = spark.sql(f"""SELECT NHS_NUMBER_DEID, DATE, LSOA, code FROM {collab_database_name}.{project_prefix}{gdppr_data}""")
gdppr = gdppr.withColumnRenamed('DATE', 'date').withColumnRenamed('NHS_NUMBER_DEID', 'person_id_deid')
gdppr = gdppr.withColumn('date_is', lit('DATE'))
gdppr = gdppr.filter((gdppr['date'] >= index_date) & (gdppr['date'] <= end_date))
gdppr.createOrReplaceGlobalTempView(f"{project_prefix}tmp_gdppr")
#display(gdppr)
drop_table(project_prefix + temp_gdppr + temp_current_initials_date, if_exists=True)
create_table(project_prefix + temp_gdppr, select_sql_script=f"SELECT * FROM global_temp.{project_prefix}tmp_gdppr") 


# COMMAND ----------

# Deaths
death = spark.sql(f"""SELECT * FROM {collab_database_name}.{project_prefix}{deaths_data}""")
death = death.withColumn("death_date", to_date(death['REG_DATE_OF_DEATH'], "yyyyMMdd"))
death = death.withColumnRenamed('DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'person_id_deid')
death = death.withColumn('date_is', lit('REG_DATE_OF_DEATH'))
death = death.filter((death['death_date'] >= index_date) & (death['death_date'] <= end_date))
death = death.filter(death['person_id_deid'].isNotNull())
death.createOrReplaceGlobalTempView(f"{project_prefix}tmp_deaths")

drop_table(project_prefix + temp_deaths + temp_current_initials_date, if_exists=True)
create_table(project_prefix + temp_deaths , select_sql_script=f"SELECT * FROM global_temp.{project_prefix}tmp_deaths") 

# COMMAND ----------

# MAGIC %md
# MAGIC ### HES APC

# COMMAND ----------

# HES APC with suspected or confirmed COVID-19
apc = spark.sql(f"""SELECT PERSON_ID_DEID, EPISTART,DIAG_4_01, DIAG_4_CONCAT, OPERTN_4_CONCAT, DISMETH, DISDEST, DISDATE, SUSRECID FROM {collab_database_name}.{project_prefix}{hes_apc_data} WHERE DIAG_4_CONCAT LIKE "%U071%" OR DIAG_4_CONCAT LIKE "%U072%" """)
apc = apc.withColumnRenamed('PERSON_ID_DEID', 'person_id_deid').withColumnRenamed('EPISTART', 'date')
apc = apc.withColumn('date_is', lit('EPISTART'))
apc = apc.filter((apc['date'] >= index_date) & (apc['date'] <= end_date))
apc = apc.filter(apc['person_id_deid'].isNotNull())
apc.createOrReplaceGlobalTempView(f"{project_prefix}tmp_apc")
#display(apc)
drop_table(project_prefix + temp_hes_apc, if_exists=True)
create_table(project_prefix + temp_hes_apc , select_sql_script=f"SELECT * FROM global_temp.{project_prefix}tmp_apc") 


# COMMAND ----------

# MAGIC %md
# MAGIC ### HES CC

# COMMAND ----------

# HES CC
cc = spark.sql(f"""SELECT * FROM {collab_database_name}.{project_prefix}{hes_cc_data} """)
cc = cc.withColumnRenamed('CCSTARTDATE', 'date').withColumnRenamed('PERSON_ID_DEID', 'person_id_deid')
cc = cc.withColumn('date_is', lit('CCSTARTDATE'))
# reformat dates for hes_cc as currently strings
asDate = udf(lambda x: datetime.strptime(x, '%Y%m%d'), DateType())
cc = cc.filter(cc['person_id_deid'].isNotNull())
cc = cc.withColumn('date', asDate(col('date')))
cc = cc.filter((cc['date'] >= index_date) & (cc['date'] <= end_date))
cc.createOrReplaceGlobalTempView(f"{project_prefix}tmp_cc")
#display(cc)
drop_table(project_prefix + temp_hes_cc, if_exists=True)
create_table(project_prefix + temp_hes_cc, select_sql_script=f"SELECT * FROM global_temp.{project_prefix}tmp_cc") 

# COMMAND ----------

# MAGIC %md
# MAGIC ### CHESS

# COMMAND ----------

chess = spark.sql(f"""
SELECT PERSON_ID_DEID as person_id_deid,
Typeofspecimen,
Covid19,
AdmittedToICU,
Highflownasaloxygen, 
NoninvasiveMechanicalventilation,
Invasivemechanicalventilation,
RespiratorySupportECMO,
DateAdmittedICU,
HospitalAdmissionDate,
InfectionSwabDate as date, 
'InfectionSwabDate' as date_is
FROM {collab_database_name}.{project_prefix}{chess_data}
""")
chess = chess.filter(chess['Covid19'] == 'Yes')
chess = chess.filter(chess['person_id_deid'].isNotNull())
chess = chess.filter((chess['date'] >= index_date) & (chess['date'] <= end_date))
chess = chess.filter(((chess['date'] >= index_date) | (chess['date'].isNull())) & ((chess['date'] <= end_date) | (chess['date'].isNull())))
chess = chess.filter(((chess['HospitalAdmissionDate'] >= index_date) | (chess['HospitalAdmissionDate'].isNull())) & ((chess['HospitalAdmissionDate'] <= end_date) | (chess['HospitalAdmissionDate'].isNull())))
chess = chess.filter(((chess['DateAdmittedICU'] >= index_date) | (chess['DateAdmittedICU'].isNull())) & ((chess['DateAdmittedICU'] <= end_date) | (chess['DateAdmittedICU'].isNull())))
chess.createOrReplaceGlobalTempView(f"{project_prefix}tmp_chess")
#display(chess)
drop_table(project_prefix + temp_chess + temp_current_initials_date, if_exists=True)
create_table(project_prefix + temp_chess, select_sql_script=f"SELECT * FROM global_temp.{project_prefix}tmp_chess") 

# COMMAND ----------

# MAGIC %md
# MAGIC ### SUS

# COMMAND ----------

## SUS
sus = spark.sql(f"""SELECT NHS_NUMBER_DEID, EPISODE_START_DATE, PRIMARY_DIAGNOSIS_CODE,
CONCAT (COALESCE(PRIMARY_DIAGNOSIS_CODE, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_1, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_2, ''), ',',
COALESCE(SECONDARY_DIAGNOSIS_CODE_3, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_4, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_5, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_6, ''), ',',
COALESCE(SECONDARY_DIAGNOSIS_CODE_7, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_8, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_9, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_10, ''), ',',
COALESCE(SECONDARY_DIAGNOSIS_CODE_11, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_12, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_13, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_14, ''), ',',
COALESCE(SECONDARY_DIAGNOSIS_CODE_15, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_16, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_17, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_18, ''), ',',
COALESCE(SECONDARY_DIAGNOSIS_CODE_19, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_20, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_21, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_22, ''), ',',
COALESCE(SECONDARY_DIAGNOSIS_CODE_23, ''), ',', COALESCE(SECONDARY_DIAGNOSIS_CODE_24, '')) as DIAG_CONCAT,
CONCAT (COALESCE(PRIMARY_PROCEDURE_CODE, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_1, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_2, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_1, ''),  ',',
COALESCE(SECONDARY_PROCEDURE_CODE_3, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_4, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_5, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_6, ''), ',',
COALESCE(SECONDARY_PROCEDURE_CODE_7, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_8, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_9, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_10, ''), ',',
COALESCE(SECONDARY_PROCEDURE_CODE_11, ''), ',', COALESCE(SECONDARY_PROCEDURE_CODE_12, '')) as PROCEDURE_CONCAT, PRIMARY_PROCEDURE_DATE, SECONDARY_PROCEDURE_DATE_1,
DISCHARGE_DESTINATION_HOSPITAL_PROVIDER_SPELL, DISCHARGE_METHOD_HOSPITAL_PROVIDER_SPELL, END_DATE_HOSPITAL_PROVIDER_SPELL
FROM {collab_database_name}.{project_prefix}{sus_data}
""")

sus = sus.withColumnRenamed('NHS_NUMBER_DEID', 'person_id_deid').withColumnRenamed('EPISODE_START_DATE', 'date')
sus = sus.withColumn('date_is', lit('EPISODE_START_DATE'))
sus = sus.filter((sus['date'] >= index_date) & (sus['date'] <= end_date))
sus = sus.filter(((sus['END_DATE_HOSPITAL_PROVIDER_SPELL'] >= index_date) | (sus['END_DATE_HOSPITAL_PROVIDER_SPELL'].isNull())) & 
                     ((sus['END_DATE_HOSPITAL_PROVIDER_SPELL'] <= end_date) | (sus['END_DATE_HOSPITAL_PROVIDER_SPELL'].isNull())))
sus = sus.filter(sus['person_id_deid'].isNotNull()) # Loads of rows with missing IDs
sus = sus.filter(sus['date'].isNotNull())
sus.createOrReplaceGlobalTempView(f"{project_prefix}tmp_sus")
