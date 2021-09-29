# Databricks notebook source
# MAGIC %md # CCU002_01-D02-patient_skinny_unassembled
# MAGIC 
# MAGIC **Description** Gather together the records for each patient in primary and secondary care before they are assembled into a skinny record for CCU002_01. The output of this is a global temp View which is then used by **CCU002_01-D02-patient_skinny_record**
# MAGIC  
# MAGIC **Author(s)** Updated by Rochelle Knight for CCU002_01 from notebook by Sam Hollings and Jenny Cooper 
# MAGIC 
# MAGIC **Reviewer(s)** 
# MAGIC 
# MAGIC **Date last updated** 
# MAGIC 
# MAGIC **Date last reviewed**
# MAGIC 
# MAGIC **Data last run** 
# MAGIC 
# MAGIC **Data input** 
# MAGIC 
# MAGIC `dars_nic_391419_j3w9t_collab.ccu002_01_hes_apc_all_years`
# MAGIC 
# MAGIC  `dars_nic_391419_j3w9t_collab.ccu002_01_hes_op_all_years`
# MAGIC  
# MAGIC  `dars_nic_391419_j3w9t_collab.ccu002_01_hes_ae_all_years`
# MAGIC  
# MAGIC  `dars_nic_391419_j3w9t_collab.ccu002_01_gdppr_dars_nic_391419_j3w9t`
# MAGIC  
# MAGIC  `dars_nic_391419_j3w9t_collab.ccu002_01_deaths_dars_nic_391419_j3w9t`
# MAGIC 
# MAGIC 
# MAGIC **Data output** `global_temp.ccu002_01_patient_skinny_unassembled`
# MAGIC 
# MAGIC **Software and versions** SQL, Python
# MAGIC 
# MAGIC **Packages and versions** Not applicable

# COMMAND ----------

# MAGIC %md
# MAGIC This notebook will make a single record for each patient with the core facts about that patient, reconciled across the main datasets (primary and secondary care)

# COMMAND ----------

# MAGIC %md ### Set the values for the widgets and import common functions

# COMMAND ----------

#Datasets
hes_apc_data = 'hes_apc_all_years'
hes_op_data = 'hes_op_all_years'
hes_ae_data = 'hes_ae_all_years'
gdppr_data = 'gdppr_dars_nic_391419_j3w9t'
deaths_data = 'deaths_dars_nic_391419_j3w9t'

#Parameters 
collab_database_name = 'dars_nic_391419_j3w9t_collab'
project_prefix = 'ccu002_01_'

# COMMAND ----------

dbutils.widgets.removeAll();

# COMMAND ----------

# MAGIC %md ### Get the secondary care data for each patient
# MAGIC First pull all the patient facts from HES

# COMMAND ----------

#HES APC patient data
spark.sql(f"""
 CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}patient_skinny_all_hes_apc AS
 SELECT DISTINCT PERSON_ID_DEID as NHS_NUMBER_DEID, 
      ETHNOS as ETHNIC, 
      SEX, 
      to_date(MYDOB,'MMyyyy') as DATE_OF_BIRTH , 
      NULL as DATE_OF_DEATH, 
      EPISTART as RECORD_DATE, 
      epikey as record_id,
      "hes_apc" as dataset,
      0 as primary,
      FYEAR
  FROM {collab_database_name}.{project_prefix}{hes_apc_data}"""
)

# COMMAND ----------

#HES AE patient data
spark.sql(f"""
 CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}patient_skinny_all_hes_ae AS
  SELECT DISTINCT PERSON_ID_DEID as NHS_NUMBER_DEID, 
      ETHNOS as ETHNIC, 
      SEX, 
      date_format(date_trunc("MM", date_add(ARRIVALDATE, -ARRIVALAGE_CALC*365)),"yyyy-MM-dd") as DATE_OF_BIRTH,
      NULL as DATE_OF_DEATH, 
      ARRIVALDATE as RECORD_DATE, 
      COALESCE(epikey, aekey) as record_id,
      "hes_ae" as dataset,
      0 as primary,
      FYEAR
  FROM {collab_database_name}.{project_prefix}{hes_ae_data}""")

# COMMAND ----------

#HES OP patient data
spark.sql(f"""
 CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}patient_skinny_all_hes_op AS
  SELECT DISTINCT PERSON_ID_DEID as NHS_NUMBER_DEID, 
      ETHNOS as ETHNIC, 
      SEX,
      date_format(date_trunc("MM", date_add(APPTDATE, -APPTAGE_CALC*365)),"yyyy-MM-dd") as DATE_OF_BIRTH,
      NULL as DATE_OF_DEATH,
      APPTDATE  as RECORD_DATE,
      ATTENDKEY as record_id,
      'hes_op' as dataset,
      0 as primary,
      FYEAR
  FROM {collab_database_name}.{project_prefix}{hes_op_data}""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}patient_skinny_all_hes as
SELECT NHS_NUMBER_DEID, ETHNIC, SEX, DATE_OF_BIRTH, DATE_OF_DEATH, RECORD_DATe, record_id, dataset, primary FROM global_temp.{project_prefix}patient_skinny_all_hes_apc
UNION ALL
SELECT NHS_NUMBER_DEID, ETHNIC, SEX, DATE_OF_BIRTH, DATE_OF_DEATH, RECORD_DATe, record_id, dataset, primary FROM global_temp.{project_prefix}patient_skinny_all_hes_ae
UNION ALL
SELECT NHS_NUMBER_DEID, ETHNIC, SEX, DATE_OF_BIRTH, DATE_OF_DEATH, RECORD_DATe, record_id, dataset, primary FROM global_temp.{project_prefix}patient_skinny_all_hes_op """)

# COMMAND ----------

# MAGIC %md ## Primary care for each patient
# MAGIC Get the patients in the standard template from GDPPR
# MAGIC 
# MAGIC These values are standard for a patient across the system, so its hard to assign a date, so Natasha from primary care told me they use `REPORTING_PERIOD_END_DATE` as the date for these patient features

# COMMAND ----------

#GDPPR patient data
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}patient_skinny_gdppr_patients AS
                SELECT NHS_NUMBER_DEID, 
                      gdppr.ETHNIC, 
                      gdppr.SEX,
                      to_date(string(YEAR_OF_BIRTH),"yyyy") as DATE_OF_BIRTH,
                      to_date(string(YEAR_OF_DEATH),"yyyy") as DATE_OF_DEATH,
                      REPORTING_PERIOD_END_DATE as RECORD_DATE, -- I got this off Natasha from Primary Care
                      NULL as record_id,
                      'GDPPR' as dataset,
                      1 as primary
                FROM {collab_database_name}.{project_prefix}{gdppr_data} as gdppr""")

# COMMAND ----------

# MAGIC %md GDPPR can also store the patient ethnicity in the `CODE` column as a SNOMED code, hence we need to bring this in as another record for the patient (but with null for the other features as they come from the generic record above)

# COMMAND ----------

#GDPPR ethnicity data suing SNOMED codes
spark.sql(f"""CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}patient_skinny_gdppr_patients_SNOMED_ethnicity AS
                SELECT NHS_NUMBER_DEID, 
                      eth.PrimaryCode as ETHNIC, 
                      gdppr.SEX,
                      to_date(string(YEAR_OF_BIRTH),"yyyy") as DATE_OF_BIRTH,
                      to_date(string(YEAR_OF_DEATH),"yyyy") as DATE_OF_DEATH,
                      DATE as RECORD_DATE,
                      NULL as record_id,
                      'GDPPR_snomed_ethnicity' as dataset,
                      1 as primary
                FROM {collab_database_name}.{project_prefix}{gdppr_data} as gdppr
                      INNER JOIN dss_corporate.gdppr_ethnicity_mappings eth on gdppr.CODE = eth.ConceptId""")

# COMMAND ----------

# MAGIC %md ### Single death per patient
# MAGIC In the deaths table (Civil registration deaths), some unfortunate people are down as dying twice. Let's take the most recent death date. 

# COMMAND ----------

spark.sql(F"""CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}patient_skinny_single_patient_death AS

SELECT *
FROM
  (SELECT *,
          row_number() OVER (PARTITION BY DEC_CONF_NHS_NUMBER_CLEAN_DEID 
                             ORDER BY REG_DATE desc, REG_DATE_OF_DEATH desc,
                             S_UNDERLYING_COD_ICD10 desc 
                             ) as death_rank
    FROM {collab_database_name}.{project_prefix}{deaths_data}
    ) cte
WHERE death_rank = 1
AND DEC_CONF_NHS_NUMBER_CLEAN_DEID IS NOT NULL
AND REG_DATE_OF_DEATH_FORMATTED > '1900-01-01'
AND REG_DATE_OF_DEATH_FORMATTED <= current_date()
""")


# COMMAND ----------

# MAGIC %md ## Combine Primary and Secondary Care along with Deaths data
# MAGIC Flag some values as NULLs:
# MAGIC - DATE_OF_DEATH flag the following as like NULL: 'NULL', "" empty strings (or just spaces),  < 1900-01-01, after the current_date(), after the record_date (the person shouldn't be set to die in the future!)
# MAGIC - DATE_OF_BIRTH flag the following as like NULL: 'NULL', "" empty strings (or just spaces),  < 1900-01-01, after the current_date(), after the record_date (the person shouldn't be set to die in the future!)
# MAGIC - SEX flag the following as NULL: 'NULL', empty string, "9", "0" (9 and 0 are coded nulls, like unknown or not specified)
# MAGIC - ETHNIC flag the following as MULL: 'NULL', empty string, "9", "99", "X", "Z" - various types of coded nulls (unknown etc.)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}patient_skinny_unassembled AS
SELECT *,      
      CASE WHEN ETHNIC IS NULL or TRIM(ETHNIC) IN ("","9", "99", "X" , "Z") THEN 1 ELSE 0 END as ethnic_null,
      CASE WHEN SEX IS NULL or TRIM(SEX) IN ("", "9", "0" ) THEN 1 ELSE 0 END as sex_null,
      CASE WHEN DATE_OF_BIRTH IS NULL OR TRIM(DATE_OF_BIRTH) = "" OR DATE_OF_BIRTH < '1900-01-01' or DATE_OF_BIRTH > current_date() OR DATE_OF_BIRTH > RECORD_DATE THEN 1 ELSE 0 END as date_of_birth_null,
      CASE WHEN DATE_OF_DEATH IS NULL OR TRIM(DATE_OF_DEATH) = "" OR DATE_OF_DEATH < '1900-01-01' OR DATE_OF_DEATH > current_date() OR DATE_OF_DEATH > RECORD_DATE THEN 1 ELSE 0 END as date_of_death_null,
      CASE WHEN dataset = 'death' THEN 1 ELSE 0 END as death_table
FROM (
      SELECT  NHS_NUMBER_DEID,
              ETHNIC,
              SEX,
              DATE_OF_BIRTH,
              DATE_OF_DEATH,
              RECORD_DATE,
              record_id,
              dataset,
              primary,
              care_domain        
      FROM (
            SELECT NHS_NUMBER_DEID,
                ETHNIC,
                SEX,
                DATE_OF_BIRTH,
                DATE_OF_DEATH,
                RECORD_DATE,
                record_id,
                dataset,
                primary, 'primary' as care_domain
              FROM global_temp.{project_prefix}patient_skinny_gdppr_patients 
            UNION ALL
            SELECT NHS_NUMBER_DEID,
                ETHNIC,
                SEX,
                DATE_OF_BIRTH,
                DATE_OF_DEATH,
                RECORD_DATE,
                record_id,
                dataset,
                primary, 'primary_SNOMED_ethnicity' as care_domain
              FROM global_temp.{project_prefix}patient_skinny_gdppr_patients_SNOMED_ethnicity
            UNION ALL
            SELECT NHS_NUMBER_DEID,
                ETHNIC,
                SEX,
                DATE_OF_BIRTH,
                DATE_OF_DEATH,
                RECORD_DATE,
                record_id,
                dataset,
                primary, 'secondary' as care_domain
              FROM global_temp.{project_prefix}patient_skinny_all_hes
            UNION ALL
            SELECT DEC_CONF_NHS_NUMBER_CLEAN_DEID as NHS_NUMBER_DEID,
                Null as ETHNIC,
                Null as SEX,
                Null as DATE_OF_BIRTH,
                REG_DATE_OF_DEATH_formatted as DATE_OF_DEATH,
                REG_DATE_formatted as RECORD_DATE,
                Null as record_id,
                'death' as dataset,
                0 as primary, 'death' as care_domain
              FROM global_temp.{project_prefix}patient_skinny_single_patient_death
          ) all_patients 
          --LEFT JOIN dars_nic_391419_j3w9t.deaths_dars_nic_391419_j3w9t death on all_patients.NHS_NUMBER_DEID = death.DEC_CONF_NHS_NUMBER_CLEAN_DEID
    ) """)

# COMMAND ----------

# MAGIC %md ## Presence table - which datasets each patient was in

# COMMAND ----------

#spark.sql(f"""
#CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}patient_dataset_presence_lookup AS
#SELECT NHS_NUMBER_DEID,
#      COALESCE(deaths, 0) as deaths,
#      COALESCE(gdppr,0) as gdppr,
#      COALESCE(hes_apc, 0) as hes_apc,
#      COALESCE(hes_op, 0) as hes_op,
#      COALESCE(hes_ae, 0) as hes_ae,
#      CASE WHEN hes_ae = 1 or hes_apc=1 or hes_op = 1 THEN 1 ELSE 0 END as hes
#FROM (
#SELECT DISTINCT DEC_CONF_NHS_NUMBER_CLEAN_DEID as NHS_NUMBER_DEID, "deaths" as data_table, 1 as presence FROM {collab_database_name}.{deaths_data}{initials_date}
#union all
#SELECT DISTINCT NHS_NUMBER_DEID, "gdppr" as data_table, 1 as presence FROM global_temp.{project_prefix}patient_skinny_gdppr_patients
#union all
#-- SELECT DISTINCT NHS_NUMBER_DEID, "hes" as data_table, 1 as presence FROM global_temp.{project_prefix}patient_skinny_all_hes
#-- union all
#SELECT DISTINCT NHS_NUMBER_DEID, "hes_apc" as data_table, 1 as presence FROM global_temp.{project_prefix}patient_skinny_all_hes_apc
#union all
#SELECT DISTINCT NHS_NUMBER_DEID, "hes_ae" as data_table, 1 as presence FROM global_temp.{project_prefix}patient_skinny_all_hes_ae
#union all
#SELECT DISTINCT NHS_NUMBER_DEID, "hes_op" as data_table, 1 as presence FROM global_temp.{project_prefix}patient_skinny_all_hes_op
#)
#PIVOT (MAX(presence) FOR data_table in ("deaths", "gdppr", "hes_apc", "hes_op", "hes_ae")) """)
