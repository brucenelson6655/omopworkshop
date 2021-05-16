-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Person Queries
-- MAGIC source: https://github.com/OHDSI/OMOP-Queries/blob/master/md/Person.md

-- COMMAND ----------

use OMOP531

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Count of Patients

-- COMMAND ----------

SELECT COUNT(person_id) AS num_persons_count
FROM   person

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Number of patients of specific gender in the dataset

-- COMMAND ----------

SELECT COUNT(person_ID) AS num_persons_count

FROM Person

WHERE GENDER_CONCEPT_ID = 8532

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Number of patients grouped by gender

-- COMMAND ----------

SELECT person.GENDER_CONCEPT_ID, concept.CONCEPT_NAME AS gender_name, COUNT(person.person_ID) AS num_persons_count

    FROM person

    INNER JOIN concept ON person.GENDER_CONCEPT_ID = concept.CONCEPT_ID

    GROUP BY person.GENDER_CONCEPT_ID, concept.CONCEPT_NAME;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Number of patients grouped by year of birth

-- COMMAND ----------

SELECT year_of_birth, COUNT(person_id) AS Num_Persons_count

    FROM person

    GROUP BY year_of_birth

    ORDER BY year_of_birth;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Number of patients grouped by residence state location

-- COMMAND ----------

SELECT NVL(state, 'XX' )
AS state_abbr, count(*) as Num_Persons_count
FROM person
LEFT OUTER JOIN location
USING( location_id )
GROUP BY NVL( state, 'XX' )
ORDER BY 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Number of patients grouped by zip code of residence

-- COMMAND ----------

SELECT state, NVL( zip, '9999999' ) AS zip, count(*) Num_Persons_count

    FROM person

    LEFT OUTER JOIN location

    USING( location_id )

    GROUP BY state, NVL( zip, '9999999' )

    ORDER BY 1, 2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Number of patients by gender, stratified by year of birth

-- COMMAND ----------

SELECT gender_concept_id, c.concept_name AS gender_name, year_of_birth, COUNT(p.person_id) AS num_persons

    FROM person p

    INNER JOIN concept c ON p.gender_concept_id = c.concept_id

    GROUP BY gender_concept_id, c.concept_name, year_of_birth

    ORDER BY concept_name, year_of_birth;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Number of patients by day of the year stratified by day of birth

-- COMMAND ----------

 SELECT day_of_birth, COUNT(person_ID) AS num_persons

    FROM person

    GROUP BY day_of_birth

    ORDER BY day_of_birth;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Number of patients by month stratified by day of birth

-- COMMAND ----------

SELECT NVL(month_of_birth,1) AS month_of_year, count(*) AS num_records

    FROM person

    GROUP BY month_of_birth

    ORDER BY 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Copyright / License info of the notebook. Copyright Databricks, Inc. [2021].  The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
-- MAGIC 
-- MAGIC |Library Name|Library License|Library License URL|Library Source URL| 
-- MAGIC | :-: | :-:| :-: | :-:|
-- MAGIC | Spark|Apache-2.0 License | https://github.com/apache/spark/blob/master/LICENSE | https://github.com/apache/spark  |
-- MAGIC |OHDSI/OMOP-Queries|||https://github.com/OHDSI/OMOP-Queries|
