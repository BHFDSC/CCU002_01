# Databricks notebook source
# MAGIC %md # wrang000_functions
# MAGIC 
# MAGIC **Description** This notebook contains functions which should be of general use across many of the other notebooks. The most commonly used ones simply make and drop tables in a more controlled fashion, or display the list of tables which can be found in the database.
# MAGIC 
# MAGIC **Author(s)** Sam Hollings

# COMMAND ----------

import pandas as pd

def check_table_ownership(database, table_list=None):
    """"""
    if table_list is None:
      table_list = spark.sql(f"""SHOW TABLES in {database}""").toPandas().tableName
    df =  pd.concat([spark.sql(f"SHOW GRANT ON {database}.{table}").toPandas().query('ActionType == "OWN"') for table in table_list],axis=0)
    return df

# COMMAND ----------

def get_tables_list(database = 'dars_nic_391419_j3w9t_collab', regex = ''):
  df_tables_list = spark.sql(f"""SHOW TABLES IN {database}""").toPandas()
  df_tables_list = df_tables_list[df_tables_list['tableName'].str.contains(regex)]
  df_tables_list['table_path'] = df_tables_list.apply(lambda row: row['database'] + "." + row['tableName'], axis=1)
  
  return df_tables_list

# COMMAND ----------

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

def database_widgets(database_name = 'dars_nic_391419_j3w9t', collab_database_name='dars_nic_391419_j3w9t_collab', datawrang_database_name='dars_nic_391419_j3w9t_collab'):
  dbutils.widgets.text("database_name", database_name)
  database_name = dbutils.widgets.get('database_name')

  dbutils.widgets.text("collab_database_name", collab_database_name)
  collab_database_name = dbutils.widgets.get('collab_database_name')
  
  dbutils.widgets.text("datawrang_database_name", datawrang_database_name)
  collab_database_name = dbutils.widgets.get('datawrang_database_name')
  
  return database_name, collab_database_name, datawrang_database_name
