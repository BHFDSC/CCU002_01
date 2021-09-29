# Databricks notebook source
# MAGIC %md # get_children_covariate_notebook
# MAGIC 
# MAGIC **Description** Get Children Function for Ancestor Codes to filter to disorders for D08 covariates notebook
# MAGIC 
# MAGIC **Author(s)** Jenny Cooper, Spencer Keene

# COMMAND ----------

#modified for koalas and to save as table

# COMMAND ----------

#import pandas as pd

# COMMAND ----------

#Trying Spark version
# Merge UK and Int. versions of snomed and create a superset.
#df_snomed_uk = spark.sql("SELECT * FROM dss_corporate.snomed_ct_concepts_core_gb").withColumnRenamed('SNOMED_CT_CONCEPTS_CORE_GB_KEY', 'SNOMED_CT_CONCEPT_ID')
#df_snomed_int = spark.sql("SELECT * FROM dss_corporate.snomed_ct_concepts_core_int").withColumnRenamed('SNOMED_CT_CONCEPTS_CORE_KEY', 'SNOMED_CT_CONCEPT_ID')
#df_snomed= df_snomed_uk.union(df_snomed_int)


# COMMAND ----------

import databricks.koalas as ks

# Merge UK and Int. versions of snomed and create a superset.
df_snomed_uk = spark.table("dss_corporate.snomed_ct_concepts_core_gb").withColumnRenamed('SNOMED_CT_CONCEPTS_CORE_GB_KEY', 'SNOMED_CT_CONCEPT_ID').to_koalas()
df_snomed_int = spark.table("dss_corporate.snomed_ct_concepts_core_int").withColumnRenamed('SNOMED_CT_CONCEPTS_CORE_KEY', 'SNOMED_CT_CONCEPT_ID').to_koalas()
df_snomed = ks.concat([df_snomed_uk, df_snomed_int], sort=False)
df_snomed.shape

# COMMAND ----------

#Trying Spark version
#df_snomed_sct_rel = spark.table("dss_corporate.snomed_ct_rf2_relationships")

# COMMAND ----------

# Load the concept relationships table.
df_snomed_sct_rel = spark.table("dss_corporate.snomed_ct_rf2_relationships").to_koalas()
df_snomed_sct_rel.shape

# COMMAND ----------

#type_ID is_a
#Use this function derived using the relationships table (df_snomed_sct_rel) on the df_SNOMED table which contains all concept IDs

def get_all_children(concept, recursive=False):
  """
  Recursively fetch child concepts based on the "IS_A"
  relationship.
  Input: Concept identifier
  Output: list
  """
  
  list_of_children = [ ]

  def dfs(concept):
      m = (df_snomed_sct_rel['DESTINATION_ID']==concept)&(df_snomed_sct_rel['TYPE_ID']=='116680003')&(df_snomed_sct_rel['ACTIVE']=='1')
      child_ids = df_snomed_sct_rel[m]['SOURCE_ID']

      if len(child_ids) == 0:#child_ids.empty:
          return

      for child_id in child_ids.to_list():
          if child_id not in list_of_children:
              list_of_children.append(child_id)

              if recursive == True:
                  dfs(child_id)


  dfs(concept)
  return list(set(list_of_children))

# COMMAND ----------

# MAGIC %md **  spark friendly code below**

# COMMAND ----------

def sam_get_all_children(concept, recursive=False):
    """Author: Sam Hollings"""
    import databricks.koalas as ks
    import pyspark.sql.functions as f
    spark.sql("USE dars_nic_391419_j3w9t_collab")
    
    df_snomed_sct_rel = spark.table("dss_corporate.snomed_ct_rf2_relationships")
  
    _verbosity = 1 # whether to print generations

    # cut down the snomed rel table to just "is a" and then cache to pull it in memory and make the query faster
    df_snomed_rel_is_a = df_snomed_sct_rel.where(f.expr(f"TYPE_ID = '116680003' AND ACTIVE = 1"))[['SOURCE_ID','DESTINATION_ID']]
    df_snomed_rel_is_a.cache().count()

    # get initial list of children
    df_children = df_snomed_rel_is_a.where(f.expr(f"DESTINATION_ID = '{concept}'")).select('SOURCE_ID').distinct()

    # add these to the output dataframe
    df_children_final = df_children
    
    import uuid
    random_id = str(uuid.uuid4()).replace("-","_")
    #df_children_final.cache().count() 
    df_children_final.write.format("delta").saveAsTable(f"temp_children_final_{random_id}")
    df_children_final = spark.table(f"temp_children_final_{random_id}")
    
    # get the count of children, to see if we need to go another layer down
    count = df_children.count()
    n = 1

    print(f"Generation: {n}: children = {count}, total list of children = {count}")
    
    if recursive:
      # if there are children:
      while count > 0:
        n=n+1
        # get the next level of children for all records simultaniously using a broadcast join and discard any children already in the final list
        df_children_of_children = (df_snomed_rel_is_a
                                   .join(f.broadcast(df_children.withColumnRenamed('SOURCE_ID','DESTINATION_ID')), 
                                         ['DESTINATION_ID'], # ON
                                         how='inner')
                                   .select('SOURCE_ID').distinct()
                                   .join(df_children_final.hint("broadcast"), ['SOURCE_ID'], how='leftanti') # remove Children which are already in the final list
                                  )
        #df_children_of_children.cache().count() # this "saves" the dataframe
        df_children_of_children.write.format("delta").mode('overwrite').saveAsTable(f"temp_children_of_children_{random_id}")
        df_children_of_children = spark.table(f"temp_children_of_children_{random_id}")

        # add these children to the total list of children
        #df_children_final = df_children_final.union(df_children_of_children)
        #df_children_final.cache().count() # this just "saves" the dataframe
        df_children_of_children.write.format("delta").mode("append").saveAsTable(f"temp_children_final_{random_id}")
        df_children_final = spark.table(f"temp_children_final_{random_id}")
        
        # set these children as the new "parent", so we can work out their children
        df_children = df_children_of_children

        # count the number of children_of_children -> if it's zero we cans stop
        count = df_children_of_children.count()
        total_count = df_children_final.count()

        if _verbosity > 0:
          print(f"Generation: {n}: children = {count}, total list of children = {total_count}")
    
    temp_table_paths = [f"temp_children_final_{random_id}", f"temp_children_of_children_{random_id}"]
    
    return df_children_final.distinct(), temp_table_paths

# COMMAND ----------

concept='64572001' #this code is equal to disorder
childConcepts, temp_table_paths = sam_get_all_children(concept, True)
childConcepts.count()

# COMMAND ----------

# MAGIC %md **Sam H:** I also changed the way you then pull the SNOMED record - doing a JOIN is better than doing an IS IN

# COMMAND ----------

display(childConcepts)

# COMMAND ----------

import pyspark.sql.functions as f
df = df_snomed.to_spark().join(f.broadcast(childConcepts.withColumnRenamed('SOURCE_ID','CONCEPT_ID')), ['CONCEPT_ID'], how='inner')
df.createOrReplaceTempView("disorder_lookup") #so now global_temp.disorder_lookup for use in other notebook
spark.sql("create table dars_nic_391419_j3w9t_collab.ccu002_vac_disorder_lookup_full as select * from disorder_lookup")
spark.sql("ALTER TABLE dars_nic_391419_j3w9t_collab.ccu002_vac_disorder_lookup_full OWNER TO dars_nic_391419_j3w9t_collab")

# COMMAND ----------

# MAGIC %md **clean up**

# COMMAND ----------

for table in temp_table_paths:
  spark.sql(f"DROP TABLE {table}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CLEAR CACHE

# COMMAND ----------

#from pyspark.sql import *
#from pyspark.sql.types import StringType

# COMMAND ----------

concept='64572001' #this code is equal to disorder
childConcepts = get_all_children(concept, True)
df = df_snomed[df_snomed['CONCEPT_ID'].isin(childConcepts)]

#df=spark.createDataFrame(df)
#df.createGlobalTempView("disorder_lookup") #so now global_temp.disorder_lookup for use in other notebook

#Saved as a permanent table to make quicker if needed (probably need to go for this option if it takes ages to run the function):
df.createOrReplaceTempView("disorder_lookup") #so now global_temp.disorder_lookup for use in other notebook
spark.sql("create table dars_nic_391419_j3w9t_collab.ccu002_vac_disorder_lookup_full as select * from disorder_lookup")

#ALTER TABLE OWNER TO
spark.sql("ALTER TABLE dars_nic_391419_j3w9t_collab.ccu002_vac_disorder_lookup_full OWNER TO dars_nic_391419_j3w9t_collab")

# COMMAND ----------

#df.createOrReplaceTempView("disorder_lookup") #so now global_temp.disorder_lookup for use in other notebook
#spark.sql("create table dars_nic_391419_j3w9t_collab.ccu002_disorder_lookup as select * from disorder_lookup")

# COMMAND ----------

#dbutils.notebook.exit("disorder_lookup")
