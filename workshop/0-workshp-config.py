# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop Configuration
# MAGIC All notebooks on this workshop have been tested to work on databricks runtime `8.0ML`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup paths

# COMMAND ----------

import os

# COMMAND ----------

# DBTITLE 0,set up default paths
delta_root_path='/dbfs/mnt/gwas-test/omop/delta/synthea/'
databasePath=delta_root_path+'/silver/OMOP531'
vocabPath='/dbfs/mnt/gwas-test/omopvocab/'
synthea_path='/dbfs/mnt/gwas-test/synthea/100K'

# COMMAND ----------

os.environ["delta_root_path"]=delta_root_path
os.environ["databasePath"]=databasePath
os.environ["vocabPath"]=vocabPath

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p $delta_root_path
# MAGIC mkdir -p $databasePath
# MAGIC mkdir -p $vocabPath

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 2. Download vocabularies
# MAGIC For this workshop, we download OMOP CDM vocabularies. To accelerate the process, this notebook automatically downloads vocabulary tables and configures paths for the rest of 
# MAGIC the workshop. You can also download vocabularies from [Athena](https://athena.ohdsi.org/search-terms/start) website.
# MAGIC 
# MAGIC <img align="right" width="700"  src="https://drive.google.com/uc?export=view&id=16TU2l7XHjQLugmS_McXegBXKMglD--Fr">

# COMMAND ----------

# MAGIC %sh
# MAGIC cd $vocabPath
# MAGIC wget https://amir-hls.s3.us-east-2.amazonaws.com/public/omop/OMOP-VOCAB.tar.gz

# COMMAND ----------

# MAGIC %sh
# MAGIC cd $vocabPath
# MAGIC tar -xf OMOP-VOCAB.tar.gz

# COMMAND ----------

# MAGIC %sh
# MAGIC cd $vocabPath
# MAGIC mv ./dbfs/FileStore/omopvocab/* .

# COMMAND ----------

# MAGIC %sh
# MAGIC ls $vocabPath
