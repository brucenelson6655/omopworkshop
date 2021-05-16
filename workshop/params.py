# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC The fields below are all dbfs mounts .. Do not use "/dbfs", you don't need to add the /dbfs for local files - thats taken care of for you. Do no use any dbfs:/ names as well - since all filesystems are assumed to be fuse mounted to the workspace. See https://docs.microsoft.com/en-us/azure/databricks/data/databricks-file-system#--mount-object-storage-to-dbfs for how to mount.

# COMMAND ----------

# MAGIC %py
# MAGIC delta_root_path='/mnt/gwas-test/omop/delta/synthea/'
# MAGIC databasePath=delta_root_path+'silver/OMOP531'
# MAGIC vocabPath='/mnt/gwas-test/omopvocab/'
# MAGIC synthea_path='/mnt/gwas-test/synthea/100K'
# MAGIC utility_path='/mnt/gwas-test/synthea'

# COMMAND ----------

# MAGIC %py
# MAGIC print("delta_root_path " + delta_root_path)
# MAGIC print("databasePath " + databasePath)
# MAGIC print("vocabPath " + vocabPath)
# MAGIC print("synthea_path " + synthea_path)
# MAGIC print("utility_path " + utility_path)
