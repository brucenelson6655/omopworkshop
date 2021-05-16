-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Observation
-- MAGIC source: https://github.com/OHDSI/OMOP-Queries/blob/master/md/Observation.md

-- COMMAND ----------

USE OMOP531

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Find a Observation from a keyword
-- MAGIC This query enables the search of LOINC and UCUM descriptions that are used in the observation domain of the vocabulary by keyword. It does not require prior knowledge of where in the logic of the vocabularies the entity is situated.
-- MAGIC 
-- MAGIC The following is a sample run of the query to run a search of the Observation domain for keyword 'LDL'. The input parameters are highlighted in blue.

-- COMMAND ----------

SELECT  T.Entity_Concept_Id,
	        T.Entity_Name,
	        T.Entity_Code,
	        T.Entity_Type,
	        T.Entity_concept_class_id,
	        T.Entity_vocabulary_id,
	        T.Entity_vocabulary_name
	FROM   (
	       SELECT  C.concept_id       Entity_Concept_Id,
	               C.concept_name     Entity_Name,
	               C.concept_code     Entity_Code,
	               'Concept'          Entity_Type,
	               C.concept_class_id    Entity_concept_class_id,
	               C.vocabulary_id    Entity_vocabulary_id,
	               V.vocabulary_name  Entity_vocabulary_name,
	               C.valid_start_date,
	               C.valid_end_date
	       FROM    concept         C, 
	               vocabulary      V
	       WHERE  C.vocabulary_id IN ('LOINC', 'UCUM')
	       AND    C.concept_class_id IS NOT NULL
	       AND    C.standard_concept = 'S'
	       AND    C.vocabulary_id = V.vocabulary_id
	       ) T
	WHERE  locate(LOWER(REPLACE(REPLACE(T.Entity_Name, ' ', ''), '-', '')), 
	             LOWER(REPLACE(REPLACE('LDL' , ' ', ''), '-', ''))) > 0
	AND     current_date() BETWEEN T.valid_start_date AND T.valid_end_date

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Copyright / License info of the notebook. Copyright Databricks, Inc. [2021].  The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
-- MAGIC 
-- MAGIC |Library Name|Library License|Library License URL|Library Source URL| 
-- MAGIC | :-: | :-:| :-: | :-:|
-- MAGIC | Spark|Apache-2.0 License | https://github.com/apache/spark/blob/master/LICENSE | https://github.com/apache/spark  |
-- MAGIC |OHDSI/OMOP-Queries|||https://github.com/OHDSI/OMOP-Queries|
