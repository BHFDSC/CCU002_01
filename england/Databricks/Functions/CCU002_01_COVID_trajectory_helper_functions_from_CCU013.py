# Databricks notebook source
def create_table(table_name:str, database_name:str='dars_nic_391419_j3w9t_collab', select_sql_script:str=None, if_not_exists=True) -> None:
  """Will save to table from a global_temp view of the same name as the supplied table name (if no SQL script is supplied)
  Otherwise, can supply a SQL script and this will be used to make the table with the specificed name, in the specifcied database."""
  
  spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
  
  if select_sql_script is None:
    select_sql_script = f"SELECT * FROM global_temp.{table_name}"
  
  if if_not_exists is True:
    if_not_exists_script=' IF NOT EXISTS'
  else:
    if_not_exists_script=''
  
  spark.sql(f"""CREATE TABLE {if_not_exists_script} {database_name}.{table_name} AS
                {select_sql_script}
             """)
  spark.sql(f"ALTER TABLE {database_name}.{table_name} OWNER TO {database_name}")
  
def drop_table(table_name:str, database_name:str='dars_nic_391419_j3w9t_collab', if_exists=True):
  if if_exists:
    IF_EXISTS = 'IF EXISTS'
  else: 
    IF_EXISTS = ''
  spark.sql(f"DROP TABLE {IF_EXISTS} {database_name}.{table_name}")

# COMMAND ----------

from pyspark.sql.functions import lit, col, udf
from functools import reduce
from pyspark.sql import DataFrame
from datetime import datetime
from pyspark.sql.types import DateType

asDate = udf(lambda x: datetime.strptime(x, '%Y-%m-%d'), DateType())



# Pyspark has no .shape() function therefore define own
import pyspark

def spark_shape(self):
    return (self.count(), len(self.columns))
pyspark.sql.dataframe.DataFrame.shape = spark_shape



# Checking for duplicates
def checkOneRowPerPt(table, id):
  n = table.select(id).count()
  n_distinct = table.select(id).dropDuplicates([id]).count()
  print("N. of rows:", n)
  print("N. distinct ids:", n_distinct)
  print("Duplicates = ", n - n_distinct)
  if n > n_distinct:
    raise ValueError('Data has duplicates!')

# COMMAND ----------

# From Alex Handy in `Cohort creation for CCU003 direct effects study` notebook
# helper functions to create phenotype covariates from GDPPR and HES given a codelist

import pyspark.sql.functions as f
from functools import reduce

def unique_codetype_givendisease(disease_start, master_codelist):
  print(master_codelist.filter((master_codelist.name.startswith(disease_start)) ).select("terminology").distinct().show())

def get_codelist_from_master(disease_start, ICD10_or_SNOMED, master_codelist):
  df = master_codelist.withColumn('code', f.regexp_replace("code", '[.]', ''))
  df = df.filter((df.name.startswith(disease_start)) & (df.terminology.startswith(ICD10_or_SNOMED)) ).select("code")
  return df

def list_medcodes(codelist_column_df):
  codelist = [item.code for item in codelist_column_df.select('code').collect()]
  return codelist

def mk_gdppr_covariate_flag(gdppr, prior_to_date_string, CONDITION_GDPPR_string, codelist, after_date_string=None):
    gdppr_tmp = gdppr.withColumnRenamed('NHS_NUMBER_DEID', 'ID').select(['ID', "DATE", "RECORD_DATE", "CODE"]).filter(f.col("DATE") < prior_to_date_string)
    if after_date_string:
      gdppr_tmp = gdppr_tmp.filter(f.col("DATE") > after_date_string)
      print(gdppr_tmp.agg({"DATE": "min"}).toPandas())
    gdppr_cov = gdppr_tmp.withColumn(CONDITION_GDPPR_string, f.when(f.col("CODE").cast('string').isin(codelist), 1).otherwise(0))
    gdppr_cov =  gdppr_cov.groupBy("ID").agg(f.max(CONDITION_GDPPR_string).alias(CONDITION_GDPPR_string))
    return gdppr_cov

def mk_hes_covariate_flag(hes, prior_to_date_string, CONDITION_HES_string, codelist, after_date_string=None):
  hes_tmp = hes.withColumnRenamed('PERSON_ID_DEID', 'ID').select(['ID', "EPISTART", "DIAG_4_CONCAT"]).filter(f.col("EPISTART") < prior_to_date_string)
  if after_date_string:
    hes_tmp = hes_tmp.filter(f.col("EPISTART") > after_date_string)
    print(hes_tmp.agg({"EPISTART": "min"}).toPandas())
#   in case of F05.1 --> F051
  hes_tmp = hes_tmp.withColumn("DIAG_4_CONCAT",f.regexp_replace(f.col("DIAG_4_CONCAT"), "\\.", ""))
  hes_cov = hes_tmp.where(
      reduce(lambda a, b: a|b, (hes_tmp['DIAG_4_CONCAT'].like('%'+code+"%") for code in codelist))
  ).select(["ID"]).withColumn(CONDITION_HES_string, f.lit(1))
  return hes_cov

def join_gdppr_hes_covariateflags(gdppr_cov, hes_cov, EVER_CONDITION_string, CONDITION_GDPPR_string, CONDITION_HES_string):
  df = gdppr_cov.join(hes_cov, "ID", "outer").na.fill(0)
  df = df.withColumn(EVER_CONDITION_string, f.when((f.col(CONDITION_GDPPR_string)+f.col(CONDITION_HES_string) )>0, 1).otherwise(0))
  df = df.filter(df[EVER_CONDITION_string] == 1).groupBy("ID").agg(f.max(EVER_CONDITION_string).alias(EVER_CONDITION_string))
  return df

def add_phenotype_to_analysis_table(analysis_table, pheno_table, EVER_CONDITION_string):
  df = analysis_table.join(pheno_table, "ID", "left").select("*")
  df = df.withColumn(EVER_CONDITION_string, f.when((f.col(EVER_CONDITION_string).isNotNull()), 1).otherwise(0))
  return df
