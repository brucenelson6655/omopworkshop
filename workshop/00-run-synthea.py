# Databricks notebook source
# MAGIC %md
# MAGIC # Simulating patient records with <img src="https://synthetichealth.github.io/synthea/assets/img/logo/synthea_sprites-1-long-trans.png", width=300 >[...](https://synthetichealth.github.io/synthea/)
# MAGIC 
# MAGIC 
# MAGIC `Synthea(TM) is a Synthetic Patient Population Simulator. The goal is to output synthetic, realistic (but not real), patient data and associated health records in a variety of formats.`
# MAGIC In this notebook, we show how to simulate patient records in parallele for patients accross the US. You can modify the code for your experiments

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cluster setup
# MAGIC To run synthea in parallel, we recommend using a single node cluster with multiple cores. See [this blog](https://databricks.com/blog/2020/10/19/announcing-single-node-clusters-on-databricks.html) for more information.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use the 00-run-sythea_serial if you are having issues with this notebook causing spark to fail .. usually on sockets or synthea. 

# COMMAND ----------

# MAGIC %sh
# MAGIC git clone https://github.com/synthetichealth/synthea.git
# MAGIC cd ./synthea
# MAGIC ./gradlew build check test

# COMMAND ----------

# MAGIC %sh
# MAGIC wget https://fieldengwebstore.z5.web.core.windows.net/bruce.nelson/data/demographics.csv -P /dbfs/mnt/gwas-test/synthea

# COMMAND ----------

import pandas as pd
df=pd.read_csv("/dbfs/mnt/gwas-test/synthea/demographics.csv")[['STNAME','TOT_POP']].groupby('STNAME').sum()
df['STATE']=df.index

# COMMAND ----------

import subprocess
from subprocess import PIPE
import os

# I have put a limiter on the pop_size of 2000 .. feel free to change as needed

def run_synthea(state,pop_size):
  if pop_size > 2000 : 
    pop_size = 2000
  synth_out='/dbfs/mnt/gwas-test/synthea/100K'
  run_params={"-p": str(pop_size),
   "--exporter.fhir.export":"false",
   "--exporter.csv.export": "true",
   "--exporter.baseDirectory":synth_out+'/'+state,
   "--exporter.csv.folder_per_run": "true",
   "--generate.log_patients.detail": "none"
  }
  
  command=["./run_synthea", state]
  options=[param for params in run_params.items() for param in params]
  
  p1=subprocess.Popen(command+options,stdout=PIPE,stderr=PIPE,cwd="./synthea")
#   p2=subprocess.Popen(["./synthea/run_synthea", "%s"%state, "-p", "%s"%pop_size, "--exporter.fhir.export", "false","--exporter.csv.export", "true", "--exporter.baseDirectory","%s-%s"%(synth_out,state), "--exporter.csv.folder_per_run", "true", "--generate.log_patients.detail", "none"],stderr=PIPE)
#   code1 = p1.returncode()
  stdout1, stderr1 = p1.communicate()
  return(stdout1,stderr1)

# COMMAND ----------

n_samples=1e5
scale=int(300e6//n_samples)
# st_list = []

# COMMAND ----------

print(scale)
print(n_samples)

# COMMAND ----------

# df1=df[[s not in st_list for s in df['STATE']]]

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import udf, col
@udf
def run_synthea_udf(s,p):
  return(run_synthea(s,p//scale))
              
spark.createDataFrame(df).repartition(50).select(run_synthea_udf('STATE','TOT_POP')).collect()
