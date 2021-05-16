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

# MAGIC %run ./params

# COMMAND ----------

os.environ["delta_root_path"]=f"/dbfs{delta_root_path}"
os.environ["databasePath"]=f"/dbfs{databasePath}"
os.environ["vocabPath"]=f"/dbfs{vocabPath}"
os.environ["utilityPath"]=f"/dbfs{utility_path}"

# COMMAND ----------

# MAGIC %sh
# MAGIC git clone https://github.com/synthetichealth/synthea.git
# MAGIC cd ./synthea
# MAGIC ./gradlew build check test

# COMMAND ----------

# MAGIC %sh
# MAGIC wget https://fieldengwebstore.z5.web.core.windows.net/bruce.nelson/data/demographics.csv -P $utilityPath

# COMMAND ----------

import pandas as pd
df=pd.read_csv(f"/dbfs{utility_path}/demographics.csv")[['STNAME','TOT_POP']].groupby('STNAME').sum()
df['STATE']=df.index

# COMMAND ----------

import subprocess
from subprocess import PIPE
import os

def run_synthea(state,pop_size):
  synth_out=f'/dbfs{synthea_path}'
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

# df1=df[[s not in st_list for s in df['STATE']]]

# COMMAND ----------

display(df)

# COMMAND ----------

states_list = df.values.tolist()

# COMMAND ----------

scale = 4000

for i in states_list : 
  countt, state = i
  print(state)
  pop_count = int(countt / scale)
  if pop_count > 1000 : 
    pop_count = 1000
    
  print(pop_count)
  run_synthea(state,pop_count)
