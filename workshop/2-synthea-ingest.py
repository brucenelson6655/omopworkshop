# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Synthea Records to <img src="https://databricks.com/wp-content/uploads/2019/12/logo-delta-lake@2x.png"; width=100>
# MAGIC In this notebook we ingest synthetic EHR data from ~10K patients,<br>
# MAGIC simulated using [synthea](https://github.com/synthetichealth/synthea/wiki),
# MAGIC from csv files into the first layer of Delta Architecture, called the Bronze Layer.<br>
# MAGIC 
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2020/04/Architect-pic@2x-1.png"iv style="text-align: center"; width=800>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------

# MAGIC %md
# MAGIC First we specify paths to raw data and the root directory to delta tables. These paths are already created using the `workshop-config` notebook.

# COMMAND ----------

# MAGIC %run ./params

# COMMAND ----------



# COMMAND ----------

dbutils.fs.mkdirs(delta_root_path)
print('Synthea Raw Path: {}\n Delta Output Path:{}'.format(synthea_path,delta_root_path))

# COMMAND ----------

datasets= ['allergies',
          'careplans',
          'conditions',
          'encounters',
          'imaging_studies',
          'immunizations',
          'medications',
          'observations',
          'organizations',
          'patients',
          'procedures',
          'providers',
         ]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingest CSV files as spark dataframes
# MAGIC Next we ingest all these files into spark dataframes, and take a look at the number of records in each table. Note that here, for simolicity we recursivley read each table and store the collection of dataframes as a dictionary, which makes it easier to reference the dataframes when we write them to deltalake. As you notice, we specify `inferSchema=True`, which causes a scan of the rows to infer the schema. In practice, to make the ingest faster it is recommended to specify the schema explicitly. 

# COMMAND ----------

# create a python dictionary of dataframes
df_dict = {}
for dataset in datasets:
    df_dict[dataset] = spark.read.csv('{}/*/csv/*/{}.csv'.format(synthea_path,dataset),header=True,inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's take a look at the number of rows for each table.

# COMMAND ----------

import pandas as pd
dataframes=[(x[0],x[1].count()) for x in list(df_dict.items())]
display(pd.DataFrame(dataframes,columns=['dataset','n_records']))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 3. De-identify Patient PII

# COMMAND ----------

# MAGIC %md
# MAGIC Our next step is to make sure patient PII is protected. 
# MAGIC To do this, we first define an encryption function and then apply it to all PII columns using [pandas_udf](https://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html#pandas-udfs-aka-vectorized-udfs), to mask data in a distributed manner.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import StringType
import pandas as pd
import hashlib

# COMMAND ----------

def mask_pii(pii_col: pd.Series) -> pd.Series:
    '''
    mask_pii: function that takes a pandas series and returned sha1 hash values of elements
    '''
    sha_value = pii_col.map(lambda x: hashlib.sha1(x.encode()).hexdigest())
    return sha_value

mask_pii_udf = pandas_udf(mask_pii, returnType=StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC We then use this function to mask pii columns for a given set of columns, namely:
# MAGIC 
# MAGIC `['SSN','DRIVERS','PASSPORT','PREFIX','FIRST','LAST','SUFFIX','MAIDEN','BIRTHPLACE','ADDRESS']`.

# COMMAND ----------

pii_cols=['SSN','DRIVERS','PASSPORT','PREFIX','FIRST','LAST','SUFFIX','MAIDEN','BIRTHPLACE','ADDRESS']
patients_obfuscated = df_dict['patients']
for c in pii_cols:
  masked_col_name = c+'_masked'
  patients_obfuscated = patients_obfuscated.withColumn(c,coalesce(c,lit('null'))).withColumn(masked_col_name,mask_pii_udf(c))

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have kept the linkage, we replace the original patient records with the obfuscated version.

# COMMAND ----------

df_dict['patients']=patients_obfuscated.drop(*pii_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 4. Write tables to Delta

# COMMAND ----------

try:
  dbutils.fs.ls(delta_root_path)
except:
  print('Path %s does not exist, creating path %s'%delta_root_path)
  dbutils.fs.mkdirs(delta_root_path)
print('Delta tables will be stored in %s'%delta_root_path)

# COMMAND ----------

for table_name in datasets:
  table_path = delta_root_path + '/bronze/{}'.format(table_name)
  df_dict[table_name].write.format('delta').mode("overwrite").save(table_path)

# COMMAND ----------

# MAGIC %md
# MAGIC separately we store `pii_linkage` table under the vault path

# COMMAND ----------

patients_obfuscated.select(['Id']+pii_cols).write.format('delta').mode("overwrite").save(delta_root_path + '/bronze/vault/pii_linkage')

# COMMAND ----------

display(dbutils.fs.ls(delta_root_path + '/bronze/'))

# COMMAND ----------

df=spark.read.format('delta').load(delta_root_path + '/bronze/allergies')

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Copyright / License info of the notebook. Copyright Databricks, Inc. [2021].  The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC 
# MAGIC |Library Name|Library License|Library License URL|Library Source URL| 
# MAGIC | :-: | :-:| :-: | :-:|
# MAGIC | Spark|Apache-2.0 License | https://github.com/apache/spark/blob/master/LICENSE | https://github.com/apache/spark  |
# MAGIC |Synthea|Apache License 2.0|https://github.com/synthetichealth/synthea/blob/master/LICENSE| https://github.com/synthetichealth/synthea|

# COMMAND ----------


