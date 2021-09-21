-- Databricks notebook source
CREATE WIDGET DROPDOWN OMOPVersion DEFAULT "OMOP600" CHOICES SELECT "OMOP531" UNION ALL SELECT "OMOP600"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC omop_version = getArgument('OMOPVersion')
-- MAGIC omop_version

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("USE "+omop_version);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import to_date
-- MAGIC spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
-- MAGIC spark.sql("set spark.sql.legacy.parquet.datetimeRebaseModeInWrite=LEGACY")
-- MAGIC 
-- MAGIC tablelist = ["CONCEPT","VOCABULARY","CONCEPT_ANCESTOR","CONCEPT_RELATIONSHIP","RELATIONSHIP","CONCEPT_SYNONYM","DOMAIN","CONCEPT_CLASS","DRUG_STRENGTH"]
-- MAGIC 
-- MAGIC for table in tablelist:
-- MAGIC   df = spark.read.csv('dbfs:/hls/omopvocab/'+table+'.csv', inferSchema=True, header=True, dateFormat="yyyy-MM-dd")
-- MAGIC   if table in ["CONCEPT","CONCEPT_RELATIONSHIP","DRUG_STRENGTH"] :
-- MAGIC     df = df.withColumn('valid_start_date', to_date(df.valid_start_date,'yyyy-MM-dd')).withColumn('valid_end_date', to_date(df.valid_end_date,'yyyy-MM-dd'))
-- MAGIC   df.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable(omop_version+'.'+table)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tablecount = "SELECT 'zzz' AS table, 0 as recs"
-- MAGIC for table in tablelist:
-- MAGIC   tablecount += " UNION SELECT '"+table+"', COUNT(1) FROM "+omop_version+"."+table
-- MAGIC tablecount += " ORDER BY 1"
-- MAGIC 
-- MAGIC display(spark.sql(tablecount))

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
