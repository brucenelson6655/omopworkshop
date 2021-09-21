-- Databricks notebook source
-- DBTITLE 1,Drop Existing OMOP 5.3.1 Database
DROP DATABASE IF EXISTS OMOP531 CASCADE;

-- COMMAND ----------

-- DBTITLE 1,Create OMOP 5.3.1 Lakehouse Database
CREATE DATABASE IF NOT EXISTS OMOP531 LOCATION '/FileStore/tables/OMOP531';

-- COMMAND ----------

-- DBTITLE 1,Use OMOP 5.3.1 Database
USE OMOP531;

-- COMMAND ----------

-- DBTITLE 1,Create OMOP 5.3.1 Tables
/*OMOP CDM v5.3.1 14June2018*/
CREATE
OR REPLACE TABLE CONCEPT (
  CONCEPT_ID LONG,
  CONCEPT_NAME STRING,
  DOMAIN_ID STRING,
  VOCABULARY_ID STRING,
  CONCEPT_CLASS_ID STRING,
  STANDARD_CONCEPT STRING,
  CONCEPT_CODE STRING,
  VALID_START_DATE DATE,
  VALID_END_DATE DATE,
  INVALID_REASON STRING
) USING DELTA;
CREATE
OR REPLACE TABLE VOCABULARY (
  VOCABULARY_ID STRING,
  VOCABULARY_NAME STRING,
  VOCABULARY_REFERENCE STRING,
  VOCABULARY_VERSION STRING,
  VOCABULARY_CONCEPT_ID LONG
) USING DELTA;
CREATE
OR REPLACE TABLE DOMAIN (
  DOMAIN_ID STRING,
  DOMAIN_NAME STRING,
  DOMAIN_CONCEPT_ID LONG
) USING DELTA;
CREATE
OR REPLACE TABLE CONCEPT_CLASS (
  CONCEPT_CLASS_ID STRING,
  CONCEPT_CLASS_NAME STRING,
  CONCEPT_CLASS_CONCEPT_ID LONG
) USING DELTA;
CREATE
OR REPLACE TABLE CONCEPT_RELATIONSHIP (
  CONCEPT_ID_1 LONG,
  CONCEPT_ID_2 LONG,
  RELATIONSHIP_ID STRING,
  VALID_START_DATE DATE,
  VALID_END_DATE DATE,
  INVALID_REASON STRING
) USING DELTA;
CREATE
OR REPLACE TABLE RELATIONSHIP (
  RELATIONSHIP_ID STRING,
  RELATIONSHIP_NAME STRING,
  IS_HIERARCHICAL STRING,
  DEFINES_ANCESTRY STRING,
  REVERSE_RELATIONSHIP_ID STRING,
  RELATIONSHIP_CONCEPT_ID LONG
) USING DELTA;
CREATE
OR REPLACE TABLE CONCEPT_SYNONYM (
  CONCEPT_ID LONG,
  CONCEPT_SYNONYM_NAME STRING,
  LANGUAGE_CONCEPT_ID LONG
) USING DELTA;
CREATE
OR REPLACE TABLE CONCEPT_ANCESTOR (
  ANCESTOR_CONCEPT_ID LONG,
  DESCENDANT_CONCEPT_ID LONG,
  MIN_LEVELS_OF_SEPARATION LONG,
  MAX_LEVELS_OF_SEPARATION LONG
) USING DELTA;
CREATE
OR REPLACE TABLE SOURCE_TO_CONCEPT_MAP (
  SOURCE_CODE STRING,
  SOURCE_CONCEPT_ID LONG,
  SOURCE_VOCABULARY_ID STRING,
  SOURCE_CODE_DESCRIPTION STRING,
  TARGET_CONCEPT_ID LONG,
  TARGET_VOCABULARY_ID STRING,
  VALID_START_DATE DATE,
  VALID_END_DATE DATE,
  INVALID_REASON STRING
) USING DELTA;
CREATE
OR REPLACE TABLE DRUG_STRENGTH (
  DRUG_CONCEPT_ID LONG,
  INGREDIENT_CONCEPT_ID LONG,
  AMOUNT_VALUE DOUBLE,
  AMOUNT_UNIT_CONCEPT_ID LONG,
  NUMERATOR_VALUE DOUBLE,
  NUMERATOR_UNIT_CONCEPT_ID LONG,
  DENOMINATOR_VALUE DOUBLE,
  DENOMINATOR_UNIT_CONCEPT_ID LONG,
  BOX_SIZE LONG,
  VALID_START_DATE DATE,
  VALID_END_DATE DATE,
  INVALID_REASON STRING
) USING DELTA;
CREATE
OR REPLACE TABLE COHORT_DEFINITION (
  COHORT_DEFINITION_ID LONG,
  COHORT_DEFINITION_NAME STRING,
  COHORT_DEFINITION_DESCRIPTION STRING,
  DEFINITION_TYPE_CONCEPT_ID LONG,
  COHORT_DEFINITION_SYNTAX STRING,
  SUBJECT_CONCEPT_ID LONG,
  COHORT_INITIATION_DATE DATE
) USING DELTA;
CREATE
OR REPLACE TABLE ATTRIBUTE_DEFINITION (
  ATTRIBUTE_DEFINITION_ID LONG,
  ATTRIBUTE_NAME STRING,
  ATTRIBUTE_DESCRIPTION STRING,
  ATTRIBUTE_TYPE_CONCEPT_ID LONG,
  ATTRIBUTE_SYNTAX STRING
) USING DELTA;
CREATE
OR REPLACE TABLE CDM_SOURCE (
  CDM_SOURCE_NAME STRING,
  CDM_SOURCE_ABBREVIATION STRING,
  CDM_HOLDER STRING,
  SOURCE_DESCRIPTION STRING,
  SOURCE_DOCUMENTATION_REFERENCE STRING,
  CDM_ETL_REFERENCE STRING,
  SOURCE_RELEASE_DATE DATE,
  CDM_RELEASE_DATE DATE,
  CDM_VERSION STRING,
  VOCABULARY_VERSION STRING
) USING DELTA;
CREATE
OR REPLACE TABLE METADATA (
  METADATA_CONCEPT_ID LONG,
  METADATA_TYPE_CONCEPT_ID LONG,
  NAME STRING,
  VALUE_AS_STRING STRING,
  VALUE_AS_CONCEPT_ID LONG,
  METADATA_DATE DATE,
  METADATA_DATETIME TIMESTAMP
) USING DELTA;
--ZORDER BY (PERSON_ID)
CREATE
OR REPLACE TABLE PERSON (
  PERSON_ID LONG,
  GENDER_CONCEPT_ID LONG,
  YEAR_OF_BIRTH LONG,
  MONTH_OF_BIRTH LONG,
  DAY_OF_BIRTH LONG,
  BIRTH_DATETIME TIMESTAMP,
  RACE_CONCEPT_ID LONG,
  ETHNICITY_CONCEPT_ID LONG,
  LOCATION_ID LONG,
  PROVIDER_ID LONG,
  CARE_SITE_ID LONG,
  PERSON_SOURCE_VALUE STRING,
  GENDER_SOURCE_VALUE STRING,
  GENDER_SOURCE_CONCEPT_ID LONG,
  RACE_SOURCE_VALUE STRING,
  RACE_SOURCE_CONCEPT_ID LONG,
  ETHNICITY_SOURCE_VALUE STRING,
  ETHNICITY_SOURCE_CONCEPT_ID LONG
) USING DELTA;
--ZORDER BY (PERSON_ID)
CREATE
OR REPLACE TABLE OBSERVATION_PERIOD (
  OBSERVATION_PERIOD_ID LONG,
  PERSON_ID LONG,
  OBSERVATION_PERIOD_START_DATE DATE,
  OBSERVATION_PERIOD_END_DATE DATE,
  PERIOD_TYPE_CONCEPT_ID LONG
) USING DELTA;
--ZORDER BY (PERSON_ID)
CREATE
OR REPLACE TABLE SPECIMEN (
  SPECIMEN_ID LONG,
  PERSON_ID LONG,
  SPECIMEN_CONCEPT_ID LONG,
  SPECIMEN_TYPE_CONCEPT_ID LONG,
  SPECIMEN_DATE DATE,
  SPECIMEN_DATETIME TIMESTAMP,
  QUANTITY DOUBLE,
  UNIT_CONCEPT_ID LONG,
  ANATOMIC_SITE_CONCEPT_ID LONG,
  DISEASE_STATUS_CONCEPT_ID LONG,
  SPECIMEN_SOURCE_ID STRING,
  SPECIMEN_SOURCE_VALUE STRING,
  UNIT_SOURCE_VALUE STRING,
  ANATOMIC_SITE_SOURCE_VALUE STRING,
  DISEASE_STATUS_SOURCE_VALUE STRING
) USING DELTA;
--ZORDER BY (PERSON_ID)
CREATE
OR REPLACE TABLE DEATH (
  PERSON_ID LONG,
  DEATH_DATE DATE,
  DEATH_DATETIME TIMESTAMP,
  DEATH_TYPE_CONCEPT_ID LONG,
  CAUSE_CONCEPT_ID LONG,
  CAUSE_SOURCE_VALUE STRING,
  CAUSE_SOURCE_CONCEPT_ID LONG
) USING DELTA;
--ZORDER BY (PERSON_ID)
CREATE
OR REPLACE TABLE VISIT_OCCURRENCE (
  VISIT_OCCURRENCE_ID LONG,
  PERSON_ID LONG,
  VISIT_CONCEPT_ID LONG,
  VISIT_START_DATE DATE,
  VISIT_START_DATETIME TIMESTAMP,
  VISIT_END_DATE DATE,
  VISIT_END_DATETIME TIMESTAMP,
  VISIT_TYPE_CONCEPT_ID LONG,
  PROVIDER_ID LONG,
  CARE_SITE_ID LONG,
  VISIT_SOURCE_VALUE STRING,
  VISIT_SOURCE_CONCEPT_ID LONG,
  ADMITTING_SOURCE_CONCEPT_ID LONG,
  ADMITTING_SOURCE_VALUE STRING,
  DISCHARGE_TO_CONCEPT_ID LONG,
  DISCHARGE_TO_SOURCE_VALUE STRING,
  PRECEDING_VISIT_OCCURRENCE_ID LONG
) USING DELTA;
--ZORDER BY (PERSON_ID)
CREATE
OR REPLACE TABLE VISIT_DETAIL (
  VISIT_DETAIL_ID LONG,
  PERSON_ID LONG,
  VISIT_DETAIL_CONCEPT_ID LONG,
  VISIT_DETAIL_START_DATE DATE,
  VISIT_DETAIL_START_DATETIME TIMESTAMP,
  VISIT_DETAIL_END_DATE DATE,
  VISIT_DETAIL_END_DATETIME TIMESTAMP,
  VISIT_DEATIL_TYPE_CONCEPT_ID LONG,
  PROVIDER_ID LONG,
  CARE_SITE_ID LONG,
  ADMITTING_SOURCE_CONCEPT_ID LONG,
  DISCHARGE_TO_CONCEPT_ID LONG,
  PRECEDING_VISIT_DETAIL_ID LONG,
  VISIT_DETAIL_SOURCE_VALUE STRING,
  VISIT_DEATIL_SOURCE_CONCEPT_ID LONG,
  ADMITTING_SOURCE_VALUE STRING,
  DISCHARGE_TO_SOURCE_VALUE STRING,
  VISIT_DETAIL_PARENT_ID LONG,
  VISIT_OCCURRENCE_ID LONG
) USING DELTA;
--ZORDER BY (PERSON_ID)
CREATE
OR REPLACE TABLE PROCEDURE_OCCURRENCE (
  PROCEDURE_OCCURRENCE_ID LONG,
  PERSON_ID LONG,
  PROCEDURE_CONCEPT_ID LONG,
  PROCEDURE_DATE DATE,
  PROCEDURE_DATETIME TIMESTAMP,
  PROCEDURE_TYPE_CONCEPT_ID LONG,
  MODIFIER_CONCEPT_ID LONG,
  QUANTITY LONG,
  PROVIDER_ID LONG,
  VISIT_OCCURRENCE_ID LONG,
  VISIT_DETAIL_ID LONG,
  PROCEDURE_SOURCE_VALUE STRING,
  PROCEDURE_SOURCE_CONCEPT_ID LONG,
  MODIFIER_SOURCE_VALUE STRING
) USING DELTA;
--ZORDER BY (PERSON_ID)
CREATE
OR REPLACE TABLE DRUG_EXPOSURE (
  DRUG_EXPOSURE_ID LONG,
  PERSON_ID LONG,
  DRUG_CONCEPT_ID LONG,
  DRUG_EXPOSURE_START_DATE DATE,
  DRUG_EXPOSURE_START_DATETIME TIMESTAMP,
  DRUG_EXPOSURE_END_DATE DATE,
  DRUG_EXPOSURE_END_DATETIME TIMESTAMP,
  VERBATIM_END_DATE DATE,
  DRUG_TYPE_CONCEPT_ID LONG,
  STOP_REASON STRING,
  REFILLS LONG,
  QUANTITY DOUBLE,
  DAYS_SUPPLY LONG,
  SIG STRING,
  ROUTE_CONCEPT_ID LONG,
  LOT_NUMBER STRING,
  PROVIDER_ID LONG,
  VISIT_OCCURRENCE_ID LONG,
  VISIT_DETAIL_ID LONG,
  DRUG_SOURCE_VALUE STRING,
  DRUG_SOURCE_CONCEPT_ID LONG,
  ROUTE_SOURCE_VALUE STRING,
  DOSE_UNIT_SOURCE_VALUE STRING
) USING DELTA;
--ZORDER BY (PERSON_ID)
CREATE
OR REPLACE TABLE DEVICE_EXPOSURE (
  DEVICE_EXPOSURE_ID LONG,
  PERSON_ID LONG,
  DEVICE_CONCEPT_ID LONG,
  DEVICE_EXPOSURE_START_DATE DATE,
  DEVICE_EXPOSURE_START_DATETIME TIMESTAMP,
  DEVICE_EXPOSURE_END_DATE DATE,
  DEVICE_EXPOSURE_END_DATETIME TIMESTAMP,
  DEVICE_TYPE_CONCEPT_ID LONG,
  UNIQUE_DEVICE_ID STRING,
  QUANTITY LONG,
  PROVIDER_ID LONG,
  VISIT_OCCURRENCE_ID LONG,
  VISIT_DETAIL_ID LONG,
  DEVICE_SOURCE_VALUE STRING,
  DEVICE_SOURCE_CONCEPT_ID LONG
) USING DELTA;
--ZORDER BY (PERSON_ID)
CREATE
OR REPLACE TABLE CONDITION_OCCURRENCE (
  CONDITION_OCCURRENCE_ID LONG,
  PERSON_ID LONG,
  CONDITION_CONCEPT_ID LONG,
  CONDITION_START_DATE DATE,
  CONDITION_START_DATETIME TIMESTAMP,
  CONDITION_END_DATE DATE,
  CONDITION_END_DATETIME TIMESTAMP,
  CONDITION_TYPE_CONCEPT_ID LONG,
  STOP_REASON STRING,
  PROVIDER_ID LONG,
  VISIT_OCCURRENCE_ID LONG,
  VISIT_DETAIL_ID LONG,
  CONDITION_SOURCE_VALUE STRING,
  CONDITION_SOURCE_CONCEPT_ID LONG,
  CONDITION_STATUS_SOURCE_VALUE STRING,
  CONDITION_STATUS_CONCEPT_ID LONG
) USING DELTA;
--ZORDER BY (PERSON_ID)
CREATE
OR REPLACE TABLE MEASUREMENT (
  MEASUREMENT_ID LONG,
  PERSON_ID LONG,
  MEASUREMENT_CONCEPT_ID LONG,
  MEASUREMENT_DATE DATE,
  MEASUREMENT_DATETIME TIMESTAMP,
  MEASUREMENT_TIME STRING,
  MEASUREMENT_TYPE_CONCEPT_ID LONG,
  OPERATOR_CONCEPT_ID LONG,
  VALUE_AS_NUMBER DOUBLE,
  VALUE_AS_CONCEPT_ID LONG,
  UNIT_CONCEPT_ID LONG,
  RANGE_LOW DOUBLE,
  RANGE_HIGH DOUBLE,
  PROVIDER_ID LONG,
  VISIT_OCCURRENCE_ID LONG,
  VISIT_DETAIL_ID LONG,
  MEASUREMENT_SOURCE_VALUE STRING,
  MEASUREMENT_SOURCE_CONCEPT_ID LONG,
  UNIT_SOURCE_VALUE STRING,
  VALUE_SOURCE_VALUE STRING
) USING DELTA;
--ZORDER BY (PERSON_ID)
CREATE
OR REPLACE TABLE NOTE (
  NOTE_ID LONG,
  PERSON_ID LONG,
  NOTE_DATE DATE,
  NOTE_DATETIME TIMESTAMP,
  NOTE_TYPE_CONCEPT_ID LONG,
  NOTE_CLASS_CONCEPT_ID LONG,
  NOTE_TITLE STRING,
  NOTE_TEXT STRING,
  ENCODING_CONCEPT_ID LONG,
  LANGUAGE_CONCEPT_ID LONG,
  PROVIDER_ID LONG,
  VISIT_OCCURRENCE_ID LONG,
  VISIT_DETAIL_ID LONG,
  NOTE_SOURCE_VALUE STRING
) USING DELTA;
CREATE
OR REPLACE TABLE NOTE_NLP (
  NOTE_NLP_ID LONG,
  NOTE_ID LONG,
  SECTION_CONCEPT_ID LONG,
  SNIPPET STRING,
  OFFSET STRING,
  LEXICAL_VARIANT STRING,
  NOTE_NLP_CONCEPT_ID LONG,
  NOTE_NLP_SOURCE_CONCEPT_ID LONG,
  NLP_SYSTEM STRING,
  NLP_DATE DATE,
  NLP_DATETIME TIMESTAMP,
  TERM_EXISTS STRING,
  TERM_TEMPORAL STRING,
  TERM_MODIFIERS STRING
) USING DELTA;
--ZORDER BY (PERSON_ID)
CREATE
OR REPLACE TABLE OBSERVATION (
  OBSERVATION_ID LONG,
  PERSON_ID LONG,
  OBSERVATION_CONCEPT_ID LONG,
  OBSERVATION_DATE DATE,
  OBSERVATION_DATETIME TIMESTAMP,
  OBSERVATION_TYPE_CONCEPT_ID LONG,
  VALUE_AS_NUMBER DOUBLE,
  VALUE_AS_STRING STRING,
  VALUE_AS_CONCEPT_ID LONG,
  QUALIFIER_CONCEPT_ID LONG,
  UNIT_CONCEPT_ID LONG,
  PROVIDER_ID LONG,
  VISIT_OCCURRENCE_ID LONG,
  VISIT_DETAIL_ID LONG,
  OBSERVATION_SOURCE_VALUE STRING,
  OBSERVATION_SOURCE_CONCEPT_ID LONG,
  UNIT_SOURCE_VALUE STRING,
  QUALIFIER_SOURCE_VALUE STRING
) USING DELTA;
CREATE
OR REPLACE TABLE FACT_RELATIONSHIP (
  DOMAIN_CONCEPT_ID_1 LONG,
  FACT_ID_1 LONG,
  DOMAIN_CONCEPT_ID_2 LONG,
  FACT_ID_2 LONG,
  RELATIONSHIP_CONCEPT_ID LONG
) USING DELTA;
CREATE
OR REPLACE TABLE LOCATION (
  LOCATION_ID LONG,
  ADDRESS_1 STRING,
  ADDRESS_2 STRING,
  CITY STRING,
  STATE STRING,
  ZIP STRING,
  COUNTY STRING,
  LOCATION_SOURCE_VALUE STRING
) USING DELTA;
CREATE
OR REPLACE TABLE CARE_SITE (
  CARE_SITE_ID LONG,
  CARE_SITE_NAME STRING,
  PLACE_OF_SERVICE_CONCEPT_ID LONG,
  LOCATION_ID LONG,
  CARE_SITE_SOURCE_VALUE STRING,
  PLACE_OF_SERVICE_SOURCE_VALUE STRING
) USING DELTA;
CREATE
OR REPLACE TABLE PROVIDER (
  PROVIDER_ID LONG,
  PROVIDER_NAME STRING,
  NPI STRING,
  DEA STRING,
  SPECIALTY_CONCEPT_ID LONG,
  CARE_SITE_ID LONG,
  YEAR_OF_BIRTH LONG,
  GENDER_CONCEPT_ID LONG,
  PROVIDER_SOURCE_VALUE STRING,
  SPECIALTY_SOURCE_VALUE STRING,
  SPECIALTY_SOURCE_CONCEPT_ID LONG,
  GENDER_SOURCE_VALUE STRING,
  GENDER_SOURCE_CONCEPT_ID LONG
) USING DELTA;
--ZORDER BY (PERSON_ID)
CREATE
OR REPLACE TABLE PAYER_PLAN_PERIOD (
  PAYER_PLAN_PERIOD_ID LONG,
  PERSON_ID LONG,
  PAYER_PLAN_PERIOD_START_DATE DATE,
  PAYER_PLAN_PERIOD_END_DATE DATE,
  PAYER_CONCEPT_ID LONG,
  PAYER_SOURCE_VALUE STRING,
  PAYER_SOURCE_CONCEPT_ID LONG,
  PLAN_CONCEPT_ID LONG,
  PLAN_SOURCE_VALUE STRING,
  PLAN_SOURCE_CONCEPT_ID LONG,
  SPONSOR_CONCEPT_ID LONG,
  SPONSOR_SOURCE_VALUE STRING,
  SPONSOR_SOURCE_CONCEPT_ID LONG,
  FAMILY_SOURCE_VALUE STRING,
  STOP_REASON_CONCEPT_ID LONG,
  STOP_REASON_SOURCE_VALUE STRING,
  STOP_REASON_SOURCE_CONCEPT_ID LONG
) USING DELTA;
CREATE
OR REPLACE TABLE COST (
  COST_ID LONG,
  COST_EVENT_ID LONG,
  COST_DOMAIN_ID STRING,
  COST_TYPE_CONCEPT_ID LONG,
  CURRENCY_CONCEPT_ID LONG,
  TOTAL_CHARGE DOUBLE,
  TOTAL_COST DOUBLE,
  TOTAL_PAID DOUBLE,
  PAID_BY_PAYER DOUBLE,
  PAID_BY_PATIENT DOUBLE,
  PAID_PATIENT_COPAY DOUBLE,
  PAID_PATIENT_COINSURANCE DOUBLE,
  PAID_PATIENT_DEDUCTIBLE DOUBLE,
  PAID_BY_PRIMARY DOUBLE,
  PAID_INGREDIENT_COST DOUBLE,
  PAID_DISPENSING_FEE DOUBLE,
  PAYER_PLAN_PERIOD_ID LONG,
  AMOUNT_ALLOWED DOUBLE,
  REVENUE_CODE_CONCEPT_ID LONG,
  REVENUE_CODE_SOURCE_VALUE STRING,
  DRG_CONCEPT_ID LONG,
  DRG_SOURCE_VALUE STRING
) USING DELTA;
--ZORDER BY (SUBJECT_ID)
CREATE
OR REPLACE TABLE COHORT (
  COHORT_DEFINITION_ID LONG,
  SUBJECT_ID LONG,
  COHORT_START_DATE DATE,
  COHORT_END_DATE DATE
) USING DELTA;
--ZORDER BY (SUBJECT_ID)
CREATE
OR REPLACE TABLE COHORT_ATTRIBUTE (
  COHORT_DEFINITION_ID LONG,
  SUBJECT_ID LONG,
  COHORT_START_DATE DATE,
  COHORT_END_DATE DATE,
  ATTRIBUTE_DEFINITION_ID LONG,
  VALUE_AS_NUMBER DOUBLE,
  VALUE_AS_CONCEPT_ID LONG
) USING DELTA;
--ZORDER BY (PERSON_ID)
CREATE
OR REPLACE TABLE DRUG_ERA (
  DRUG_ERA_ID LONG,
  PERSON_ID LONG,
  DRUG_CONCEPT_ID LONG,
  DRUG_ERA_START_DATE DATE,
  DRUG_ERA_END_DATE DATE,
  DRUG_EXPOSURE_COUNT LONG,
  GAP_DAYS LONG
) USING DELTA;
--ZORDER BY (PERSON_ID)
CREATE
OR REPLACE TABLE DOSE_ERA (
  DOSE_ERA_ID LONG,
  PERSON_ID LONG,
  DRUG_CONCEPT_ID LONG,
  UNIT_CONCEPT_ID LONG,
  DOSE_VALUE DOUBLE,
  DOSE_ERA_START_DATE DATE,
  DOSE_ERA_END_DATE DATE
) USING DELTA;
--ZORDER BY (PERSON_ID)
CREATE
OR REPLACE TABLE CONDITION_ERA (
  CONDITION_ERA_ID LONG,
  PERSON_ID LONG,
  CONDITION_CONCEPT_ID LONG,
  CONDITION_ERA_START_DATE DATE,
  CONDITION_ERA_END_DATE DATE,
  CONDITION_OCCURRENCE_COUNT LONG
) USING DELTA;
INSERT INTO
  metadata
VALUES
  (
    0,
    0,
    'OHDSI OMOP CDM Version',
    '5.3.1',
    0,
    CURRENT_DATE,
    CURRENT_TIMESTAMP
  );

-- COMMAND ----------

-- DBTITLE 1,Create OMOP 5.3.1 Result Tables
--ZORDER BY (subject_id)
CREATE
OR REPLACE TABLE cohort (
  cohort_definition_id LONG,
  subject_id LONG,
  cohort_start_date DATE,
  cohort_end_date DATE
) USING DELTA;
CREATE
OR REPLACE TABLE cohort_definition (
  cohort_definition_id LONG,
  cohort_definition_name STRING,
  cohort_definition_description STRING,
  definition_type_concept_id LONG,
  cohort_definition_syntax STRING,
  subject_concept_id LONG,
  cohort_initiation_date DATE
) USING DELTA;

-- COMMAND ----------

-- DBTITLE 1,Check OMOP 5.3.1 Metadata Version
SELECT
  metadata_concept_id,
  metadata_type_concept_id,
  name,
  value_as_string,
  value_as_concept_id,
  metadata_date,
  metadata_datetime
FROM
  metadata;