-- Databricks notebook source
-- MAGIC %md
-- MAGIC # OMOP Vocabulary Setup
-- MAGIC To construct vocabulary tables, you first need to download the vocabularies from [Athena](https://athena.ohdsi.org/search-terms/start) website.
-- MAGIC You need to create an account which is free and you will be able to downoad the vocabularies.
-- MAGIC Next you can use [databricks dbfs api](https://docs.databricks.com/dev-tools/api/latest/dbfs.html#dbfs-api) utilities to upload downloaded vocabularies to `dbfs` under your `vocab_path`.
-- MAGIC 
-- MAGIC <img align="right" width="700"  src="https://drive.google.com/uc?export=view&id=16TU2l7XHjQLugmS_McXegBXKMglD--Fr">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## notebook config

-- COMMAND ----------

-- MAGIC %run ./params

-- COMMAND ----------

-- MAGIC %python
-- MAGIC omop_version = "OMOP531"
-- MAGIC print("Using OMOP version %s"%omop_version)
-- MAGIC print("Using vocabulary tables in %s"%vocabPath)
-- MAGIC spark.sql("USE "+omop_version);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Loading vocabularies as delta tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import to_date
-- MAGIC spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
-- MAGIC spark.sql("set spark.sql.legacy.parquet.datetimeRebaseModeInWrite=LEGACY")
-- MAGIC 
-- MAGIC tablelist = ["CONCEPT","VOCABULARY","CONCEPT_ANCESTOR","CONCEPT_RELATIONSHIP","RELATIONSHIP","CONCEPT_SYNONYM","DOMAIN","CONCEPT_CLASS","DRUG_STRENGTH"]
-- MAGIC 
-- MAGIC for table in tablelist:
-- MAGIC 
-- MAGIC   df = spark.read.csv(vocabPath+table+'.csv.gz', inferSchema=True, header=True, dateFormat="yyyy-MM-dd")
-- MAGIC 
-- MAGIC   if table in ["CONCEPT","CONCEPT_RELATIONSHIP","DRUG_STRENGTH"] :
-- MAGIC     df = df.withColumn('valid_start_date', to_date(df.valid_start_date,'yyyy-MM-dd')).withColumn('valid_end_date', to_date(df.valid_end_date,'yyyy-MM-dd'))
-- MAGIC     
-- MAGIC   df.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable(omop_version+'.'+table)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Show tables 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tablecount = "SELECT '-' AS table, 0 as recs"
-- MAGIC for table in tablelist:
-- MAGIC   tablecount += " UNION SELECT '"+table+"', COUNT(1) FROM "+omop_version+"."+table
-- MAGIC tablecount += " ORDER BY 2 DESC"
-- MAGIC 
-- MAGIC display(spark.sql(tablecount))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create vocab map tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### source_to_standard_vocab_map

-- COMMAND ----------

DROP TABLE IF EXISTS source_to_standard_vocab_map
;
CREATE TABLE source_to_standard_vocab_map AS WITH CTE_VOCAB_MAP AS (
  SELECT
    c.concept_code AS SOURCE_CODE,
    c.concept_id AS SOURCE_CONCEPT_ID,
    c.concept_name AS SOURCE_CODE_DESCRIPTION,
    c.vocabulary_id AS SOURCE_VOCABULARY_ID,
    c.domain_id AS SOURCE_DOMAIN_ID,
    c.CONCEPT_CLASS_ID AS SOURCE_CONCEPT_CLASS_ID,
    c.VALID_START_DATE AS SOURCE_VALID_START_DATE,
    c.VALID_END_DATE AS SOURCE_VALID_END_DATE,
    c.INVALID_REASON AS SOURCE_INVALID_REASON,
    c1.concept_id AS TARGET_CONCEPT_ID,
    c1.concept_name AS TARGET_CONCEPT_NAME,
    c1.VOCABULARY_ID AS TARGET_VOCABULARY_ID,
    c1.domain_id AS TARGET_DOMAIN_ID,
    c1.concept_class_id AS TARGET_CONCEPT_CLASS_ID,
    c1.INVALID_REASON AS TARGET_INVALID_REASON,
    c1.standard_concept AS TARGET_STANDARD_CONCEPT
  FROM
    CONCEPT C
    JOIN CONCEPT_RELATIONSHIP CR ON C.CONCEPT_ID = CR.CONCEPT_ID_1
    AND CR.invalid_reason IS NULL
    AND lower(cr.relationship_id) = 'maps to'
    JOIN CONCEPT C1 ON CR.CONCEPT_ID_2 = C1.CONCEPT_ID
    AND C1.INVALID_REASON IS NULL
  UNION
  SELECT
    source_code,
    SOURCE_CONCEPT_ID,
    SOURCE_CODE_DESCRIPTION,
    source_vocabulary_id,
    c1.domain_id AS SOURCE_DOMAIN_ID,
    c2.CONCEPT_CLASS_ID AS SOURCE_CONCEPT_CLASS_ID,
    c1.VALID_START_DATE AS SOURCE_VALID_START_DATE,
    c1.VALID_END_DATE AS SOURCE_VALID_END_DATE,
    stcm.INVALID_REASON AS SOURCE_INVALID_REASON,
    target_concept_id,
    c2.CONCEPT_NAME AS TARGET_CONCEPT_NAME,
    target_vocabulary_id,
    c2.domain_id AS TARGET_DOMAIN_ID,
    c2.concept_class_id AS TARGET_CONCEPT_CLASS_ID,
    c2.INVALID_REASON AS TARGET_INVALID_REASON,
    c2.standard_concept AS TARGET_STANDARD_CONCEPT
  FROM
    source_to_concept_map stcm
    LEFT OUTER JOIN CONCEPT c1 ON c1.concept_id = stcm.source_concept_id
    LEFT OUTER JOIN CONCEPT c2 ON c2.CONCEPT_ID = stcm.target_concept_id
  WHERE
    stcm.INVALID_REASON IS NULL
)
SELECT
  *
FROM
  CTE_VOCAB_MAP
  ;
SELECT * FROM source_to_standard_vocab_map LIMIT 100
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### source_to_source_vocab_map

-- COMMAND ----------

DROP TABLE IF EXISTS source_to_source_vocab_map
;
CREATE TABLE source_to_source_vocab_map AS WITH CTE_VOCAB_MAP AS (
  SELECT
    c.concept_code AS SOURCE_CODE,
    c.concept_id AS SOURCE_CONCEPT_ID,
    c.CONCEPT_NAME AS SOURCE_CODE_DESCRIPTION,
    c.vocabulary_id AS SOURCE_VOCABULARY_ID,
    c.domain_id AS SOURCE_DOMAIN_ID,
    c.concept_class_id AS SOURCE_CONCEPT_CLASS_ID,
    c.VALID_START_DATE AS SOURCE_VALID_START_DATE,
    c.VALID_END_DATE AS SOURCE_VALID_END_DATE,
    c.invalid_reason AS SOURCE_INVALID_REASON,
    c.concept_ID as TARGET_CONCEPT_ID,
    c.concept_name AS TARGET_CONCEPT_NAME,
    c.vocabulary_id AS TARGET_VOCABULARY_ID,
    c.domain_id AS TARGET_DOMAIN_ID,
    c.concept_class_id AS TARGET_CONCEPT_CLASS_ID,
    c.INVALID_REASON AS TARGET_INVALID_REASON,
    c.STANDARD_CONCEPT AS TARGET_STANDARD_CONCEPT
  FROM
    CONCEPT c
  UNION
  SELECT
    source_code,
    SOURCE_CONCEPT_ID,
    SOURCE_CODE_DESCRIPTION,
    source_vocabulary_id,
    c1.domain_id AS SOURCE_DOMAIN_ID,
    c2.CONCEPT_CLASS_ID AS SOURCE_CONCEPT_CLASS_ID,
    c1.VALID_START_DATE AS SOURCE_VALID_START_DATE,
    c1.VALID_END_DATE AS SOURCE_VALID_END_DATE,
    stcm.INVALID_REASON AS SOURCE_INVALID_REASON,
    target_concept_id,
    c2.CONCEPT_NAME AS TARGET_CONCEPT_NAME,
    target_vocabulary_id,
    c2.domain_id AS TARGET_DOMAIN_ID,
    c2.concept_class_id AS TARGET_CONCEPT_CLASS_ID,
    c2.INVALID_REASON AS TARGET_INVALID_REASON,
    c2.standard_concept AS TARGET_STANDARD_CONCEPT
  FROM
    source_to_concept_map stcm
    LEFT OUTER JOIN CONCEPT c1 ON c1.concept_id = stcm.source_concept_id
    LEFT OUTER JOIN CONCEPT c2 ON c2.CONCEPT_ID = stcm.target_concept_id
  WHERE
    stcm.INVALID_REASON IS NULL
)
SELECT
  *
FROM
  CTE_VOCAB_MAP
;
SELECT * FROM source_to_source_vocab_map LIMIT 100
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Copyright / License info of the notebook. Copyright Databricks, Inc. [2021].  The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
-- MAGIC 
-- MAGIC |Library Name|Library License|Library License URL|Library Source URL| 
-- MAGIC | :-: | :-:| :-: | :-:|
-- MAGIC | Spark|Apache-2.0 License | https://github.com/apache/spark/blob/master/LICENSE | https://github.com/apache/spark  |
-- MAGIC | OHDSI/ETL-Synthea| Apache License 2.0 | https://github.com/OHDSI/ETL-Synthea/blob/master/LICENSE | https://github.com/OHDSI/ETL-Synthea |
