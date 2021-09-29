# Databricks notebook source
# MAGIC %md # covariate_functions
# MAGIC 
# MAGIC **Description** Functions for covariates notebook D08
# MAGIC 
# MAGIC **Author(s)** Spencer Keene

# COMMAND ----------

def unique_codetype_givendisease(disease_start, master_codelist):
  print(master_codelist.filter((master_codelist.name.startswith(disease_start)) ).select("terminology").distinct().show())

def get_codelist_from_master(disease_start, ICD10_or_SNOMED, master_codelist):
  df = master_codelist.withColumn('code', regexp_replace("code", '[.]', ''))
  df = df.filter((df.name.startswith(disease_start)) & (df.terminology.startswith(ICD10_or_SNOMED)) ).select("code")
  return df

def get_codelist_from_master_sjk(disease_start, ICD10_or_SNOMED, master_codelist_sjk):
  df = master_codelist_sjk.withColumn('code', regexp_replace("code", '[.]', ''))
  df = df.filter((df.name.startswith(disease_start)) & (df.terminology.startswith(ICD10_or_SNOMED)) ).select("code")
  return df

def list_medcodes(codelist_column_df):
  codelist = [item.code for item in codelist_column_df.select('code').collect()]
  return codelist

def mk_gdppr_covariate_flag(gdppr, prior_to_date_string, CONDITION_GDPPR_string, codelist, after_date_string=None):
    gdppr_tmp = gdppr.withColumnRenamed('NHS_NUMBER_DEID', 'ID').select(['ID', "DATE", "RECORD_DATE", "CODE"]).filter(col("DATE") < prior_to_date_string)
    if after_date_string:
      gdppr_tmp = gdppr_tmp.filter(col("DATE") > after_date_string)
      print(gdppr_tmp.agg({"DATE": "min"}).toPandas())
    gdppr_cov = gdppr_tmp.withColumn(CONDITION_GDPPR_string, when(col("CODE").cast('string').isin(codelist), 1).otherwise(0))
    gdppr_cov =  gdppr_cov.groupBy("ID").agg(f.max(CONDITION_GDPPR_string).alias(CONDITION_GDPPR_string))
    return gdppr_cov

def mk_hes_covariate_flag(hes, prior_to_date_string, CONDITION_HES_string, codelist, after_date_string=None):
  hes_tmp = hes.withColumnRenamed('PERSON_ID_DEID', 'ID').select(['ID', "EPISTART", "DIAG_4_CONCAT"]).filter(col("EPISTART") < prior_to_date_string)
  if after_date_string:
    hes_tmp = hes_tmp.filter(col("EPISTART") > after_date_string)
    print(hes_tmp.agg({"EPISTART": "min"}).toPandas())
#   in case of F05.1 --> F051
  hes_tmp = hes_tmp.withColumn("DIAG_4_CONCAT",f.regexp_replace(col("DIAG_4_CONCAT"), "\\.", ""))
  hes_cov = hes_tmp.where(
      reduce(lambda a, b: a|b, (hes_tmp['DIAG_4_CONCAT'].like('%'+code+"%") for code in codelist))
  ).select(["ID"]).withColumn(CONDITION_HES_string, lit(1))
  return hes_cov

def join_gdppr_hes_covariateflags(gdppr_cov, hes_cov, EVER_CONDITION_string, CONDITION_GDPPR_string, CONDITION_HES_string):
  df = gdppr_cov.join(hes_cov, "ID", "outer").na.fill(0)
  df = df.withColumn(EVER_CONDITION_string, when((col(CONDITION_GDPPR_string)+col(CONDITION_HES_string) )>0, 1).otherwise(0))
  return df
  
def examine_rows_column_value(df, colname, value):
    if value is None:
        tmp_df = df.where(col(colname).isNull())
    else:
        tmp_df = df[df[colname] == value]
    display(tmp_df)

    
def count_unique_pats(df, id_colname):
    n_unique_pats = df.agg(countDistinct(id_colname)).toPandas()
    return int(n_unique_pats.values)

#def create_table(df, table_name:str, database_name:str="dars_nic_391419_j3w9t_collab", select_sql_script:str=None) -> None:
#   adapted from sam h 's save function
#  """Will save to table from a global_temp view of the same name as the supplied table name (if no SQL script is supplied)
#  Otherwise, can supply a SQL script and this will be used to make the table with the specificed name, in the specifcied database."""
#  spark.sql(f"""DROP TABLE IF EXISTS {database_name}.{table_name}""")
#  df.createOrReplaceGlobalTempView(table_name)
#  spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
#  if select_sql_script is None:
#    select_sql_script = f"SELECT * FROM global_temp.{table_name}"
#  spark.sql(f"""CREATE TABLE {database_name}.{table_name} AS
#                {select_sql_script}""")
#  spark.sql(f"""
#                ALTER TABLE {database_name}.{table_name} OWNER TO {database_name}
#             """)
#################################################  
 
def get_patids_with_drugtype(BNF_chapter_startstring, df):
    df_drugtype = df.filter(df.PrescribedBNFCode.startswith(BNF_chapter_startstring))
    display(df_drugtype)
    ids_drugtype = df_drugtype.select("Person_ID_DEID").distinct() 
    return ids_drugtype
  
  
 


# COMMAND ----------

# MAGIC %python
# MAGIC # Define create table function by Sam Hollings
# MAGIC # Source: Workspaces/dars_nic_391419_j3w9t_collab/DATA_CURATION_wrang000_functions
# MAGIC 
# MAGIC def create_table(table_name:str, database_name:str='dars_nic_391419_j3w9t_collab', select_sql_script:str=None) -> None:
# MAGIC   """Will save to table from a global_temp view of the same name as the supplied table name (if no SQL script is supplied)
# MAGIC   Otherwise, can supply a SQL script and this will be used to make the table with the specificed name, in the specifcied database."""
# MAGIC   
# MAGIC   spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
# MAGIC   
# MAGIC   if select_sql_script is None:
# MAGIC     select_sql_script = f"SELECT * FROM global_temp.{table_name}"
# MAGIC   
# MAGIC   spark.sql(f"""CREATE TABLE {database_name}.{table_name} AS
# MAGIC                 {select_sql_script}
# MAGIC              """)
# MAGIC   spark.sql(f"ALTER TABLE {database_name}.{table_name} OWNER TO {database_name}")
# MAGIC   
# MAGIC def drop_table(table_name:str, database_name:str='dars_nic_391419_j3w9t_collab', if_exists=True):
# MAGIC   if if_exists:
# MAGIC     IF_EXISTS = 'IF EXISTS'
# MAGIC   else: 
# MAGIC     IF_EXISTS = ''
# MAGIC   spark.sql(f"DROP TABLE {IF_EXISTS} {database_name}.{table_name}")
