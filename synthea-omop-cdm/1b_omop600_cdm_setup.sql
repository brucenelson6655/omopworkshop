-- Databricks notebook source
-- DBTITLE 1,Drop Existing OMOP 6.0.0 Database
DROP DATABASE IF EXISTS OMOP600 CASCADE;

-- COMMAND ----------

-- DBTITLE 1,Create OMOP 6.0.0 Lakehouse Database
CREATE DATABASE IF NOT EXISTS OMOP600 LOCATION '/FileStore/tables/OMOP600';

-- COMMAND ----------

-- DBTITLE 1,Use OMOP 6.0.0 Database
USE OMOP600;

-- COMMAND ----------

-- DBTITLE 1,Create OMOP 6.0.0 Tables
CREATE
OR REPLACE TABLE concept (
  concept_id INT,
  concept_name STRING,
  domain_id STRING,
  vocabulary_id STRING,
  concept_class_id STRING,
  standard_concept STRING,
  concept_code STRING,
  valid_start_date DATE,
  valid_end_date DATE,
  invalid_reason STRING
) USING DELTA;
CREATE
OR REPLACE TABLE vocabulary (
  vocabulary_id STRING,
  vocabulary_name STRING,
  vocabulary_reference STRING,
  vocabulary_version STRING,
  vocabulary_concept_id INT
) USING DELTA;
CREATE
OR REPLACE TABLE domain (
  domain_id STRING,
  domain_name STRING,
  domain_concept_id INT
) USING DELTA;
CREATE
OR REPLACE TABLE concept_class (
  concept_class_id STRING,
  concept_class_name STRING,
  concept_class_concept_id INT
) USING DELTA;
CREATE
OR REPLACE TABLE concept_relationship (
  concept_id_1 INT,
  concept_id_2 INT,
  relationship_id STRING,
  valid_start_date DATE,
  valid_end_date DATE,
  invalid_reason STRING
) USING DELTA;
CREATE
OR REPLACE TABLE relationship (
  relationship_id STRING,
  relationship_name STRING,
  is_hierarchical STRING,
  defines_ancestry STRING,
  reverse_relationship_id STRING,
  relationship_concept_id INT
) USING DELTA;
CREATE
OR REPLACE TABLE concept_synonym (
  concept_id INT,
  concept_synonym_name STRING,
  language_concept_id INT
) USING DELTA;
CREATE
OR REPLACE TABLE concept_ancestor (
  ancestor_concept_id INT,
  descendant_concept_id INT,
  min_levels_of_separation INT,
  max_levels_of_separation INT
) USING DELTA;
CREATE
OR REPLACE TABLE source_to_concept_map (
  source_code STRING,
  source_concept_id INT,
  source_vocabulary_id STRING,
  source_code_description STRING,
  target_concept_id INT,
  target_vocabulary_id STRING,
  valid_start_date DATE,
  valid_end_date DATE,
  invalid_reason STRING
) USING DELTA;
CREATE
OR REPLACE TABLE drug_strength (
  drug_concept_id INT,
  ingredient_concept_id INT,
  amount_value DOUBLE,
  amount_unit_concept_id INT,
  numerator_value DOUBLE,
  numerator_unit_concept_id INT,
  denominator_value DOUBLE,
  denominator_unit_concept_id INT,
  box_size INT,
  valid_start_date DATE,
  valid_end_date DATE,
  invalid_reason STRING
) USING DELTA;
CREATE
OR REPLACE TABLE cdm_source (
  cdm_source_name STRING,
  cdm_source_abbreviation STRING,
  cdm_holder STRING,
  source_description STRING,
  source_documentation_reference STRING,
  cdm_etl_reference STRING,
  source_release_date DATE,
  cdm_release_date DATE,
  cdm_version STRING,
  vocabulary_version STRING
) USING DELTA;
CREATE
OR REPLACE TABLE metadata (
  metadata_concept_id INT,
  metadata_type_concept_id INT,
  name STRING,
  value_as_string STRING,
  value_as_concept_id INT,
  metadata_date DATE,
  metadata_datetime TIMESTAMP
) USING DELTA;
--ZORDER BY (person_id)
  CREATE
  OR REPLACE TABLE person (
    person_id INT,
    gender_concept_id INT,
    year_of_birth INT,
    month_of_birth INT,
    day_of_birth INT,
    birth_datetime TIMESTAMP,
    death_datetime TIMESTAMP,
    race_concept_id INT,
    ethnicity_concept_id INT,
    location_id INT,
    provider_id INT,
    care_site_id INT,
    person_source_value STRING,
    gender_source_value STRING,
    gender_source_concept_id INT,
    race_source_value STRING,
    race_source_concept_id INT,
    ethnicity_source_value STRING,
    ethnicity_source_concept_id INT
  ) USING DELTA;
--ZORDER BY (person_id)
  CREATE
  OR REPLACE TABLE observation_period (
    observation_period_id INT,
    person_id INT,
    observation_period_start_date DATE,
    observation_period_end_date DATE,
    period_type_concept_id INT
  ) USING DELTA;
--ZORDER BY (person_id)
  CREATE
  OR REPLACE TABLE specimen (
    specimen_id INT,
    person_id INT,
    specimen_concept_id INT,
    specimen_type_concept_id INT,
    specimen_date date,
    specimen_datetime TIMESTAMP,
    quantity DOUBLE,
    unit_concept_id INT,
    anatomic_site_concept_id INT,
    disease_status_concept_id INT,
    specimen_source_id STRING,
    specimen_source_value STRING,
    unit_source_value STRING,
    anatomic_site_source_value STRING,
    disease_status_source_value STRING
  ) USING DELTA;
--ZORDER BY (person_id)
  CREATE
  OR REPLACE TABLE visit_occurrence (
    visit_occurrence_id INT,
    person_id INT,
    visit_concept_id INT,
    visit_start_date DATE,
    visit_start_datetime TIMESTAMP,
    visit_end_date DATE,
    visit_end_datetime TIMESTAMP,
    visit_type_concept_id INT,
    provider_id INT,
    care_site_id INT,
    visit_source_value STRING,
    visit_source_concept_id INT,
    admitted_from_concept_id INT,
    admitted_from_source_value STRING,
    discharge_to_source_value STRING,
    discharge_to_concept_id INT,
    preceding_visit_occurrence_id INT
  ) USING DELTA;
--ZORDER BY (person_id)
  CREATE
  OR REPLACE TABLE visit_detail (
    visit_detail_id INT,
    person_id INT,
    visit_detail_concept_id INT,
    visit_detail_start_date DATE,
    visit_detail_start_datetime TIMESTAMP,
    visit_detail_end_date DATE,
    visit_detail_end_datetime TIMESTAMP,
    visit_detail_type_concept_id INT,
    provider_id INT,
    care_site_id INT,
    discharge_to_concept_id INT,
    admitted_from_concept_id INT,
    admitted_from_source_value STRING,
    visit_detail_source_value STRING,
    visit_detail_source_concept_id INT,
    discharge_to_source_value STRING,
    preceding_visit_detail_id INT,
    visit_detail_parent_id INT,
    visit_occurrence_id INT
  ) USING DELTA;
--ZORDER BY (person_id)
  CREATE
  OR REPLACE TABLE procedure_occurrence (
    procedure_occurrence_id INT,
    person_id INT,
    procedure_concept_id INT,
    procedure_date DATE,
    procedure_datetime TIMESTAMP,
    procedure_type_concept_id INT,
    modifier_concept_id INT,
    quantity INT,
    provider_id INT,
    visit_occurrence_id INT,
    visit_detail_id INT,
    procedure_source_value STRING,
    procedure_source_concept_id INT,
    modifier_source_value STRING
  ) USING DELTA;
--ZORDER BY (person_id)
  CREATE
  OR REPLACE TABLE drug_exposure (
    drug_exposure_id INT,
    person_id INT,
    drug_concept_id INT,
    drug_exposure_start_date DATE,
    drug_exposure_start_datetime TIMESTAMP,
    drug_exposure_end_date DATE,
    drug_exposure_end_datetime TIMESTAMP,
    verbatim_end_date DATE,
    drug_type_concept_id INT,
    stop_reason STRING,
    refills INT,
    quantity DOUBLE,
    days_supply INT,
    sig STRING,
    route_concept_id INT,
    lot_number STRING,
    provider_id INT,
    visit_occurrence_id INT,
    visit_detail_id INT,
    drug_source_value STRING,
    drug_source_concept_id INT,
    route_source_value STRING,
    dose_unit_source_value STRING
  ) USING DELTA;
--ZORDER BY (person_id)
  CREATE
  OR REPLACE TABLE device_exposure (
    device_exposure_id INT,
    person_id INT,
    device_concept_id INT,
    device_exposure_start_date DATE,
    device_exposure_start_datetime TIMESTAMP,
    device_exposure_end_date DATE,
    device_exposure_end_datetime TIMESTAMP,
    device_type_concept_id INT,
    unique_device_id STRING,
    quantity INT,
    provider_id INT,
    visit_occurrence_id INT,
    visit_detail_id INT,
    device_source_value STRING,
    device_source_concept_id INT
  ) USING DELTA;
--ZORDER BY (person_id)
  CREATE
  OR REPLACE TABLE condition_occurrence (
    condition_occurrence_id INT,
    person_id INT,
    condition_concept_id INT,
    condition_start_date DATE,
    condition_start_datetime TIMESTAMP,
    condition_end_date DATE,
    condition_end_datetime TIMESTAMP,
    condition_type_concept_id INT,
    condition_status_concept_id INT,
    stop_reason STRING,
    provider_id INT,
    visit_occurrence_id INT,
    visit_detail_id INT,
    condition_source_value STRING,
    condition_source_concept_id INT,
    condition_status_source_value STRING
  ) USING DELTA;
--ZORDER BY (person_id)
  CREATE
  OR REPLACE TABLE measurement (
    measurement_id INT,
    person_id INT,
    measurement_concept_id INT,
    measurement_date DATE,
    measurement_datetime TIMESTAMP,
    measurement_time STRING,
    measurement_type_concept_id INT,
    operator_concept_id INT,
    value_as_number DOUBLE,
    value_as_concept_id INT,
    unit_concept_id INT,
    range_low DOUBLE,
    range_high DOUBLE,
    provider_id INT,
    visit_occurrence_id INT,
    visit_detail_id INT,
    measurement_source_value STRING,
    measurement_source_concept_id INT,
    unit_source_value STRING,
    value_source_value STRING
  ) USING DELTA;
--ZORDER BY (person_id)
  CREATE
  OR REPLACE TABLE note (
    note_id INT,
    person_id INT,
    note_event_id INT,
    note_event_field_concept_id INT,
    note_date DATE,
    note_datetime TIMESTAMP,
    note_type_concept_id INT,
    note_class_concept_id INT,
    note_title STRING,
    note_text STRING,
    encoding_concept_id INT,
    language_concept_id INT,
    provider_id INT,
    visit_occurrence_id INT,
    visit_detail_id INT,
    note_source_value STRING
  ) USING DELTA;
CREATE
  OR REPLACE TABLE note_nlp (
    note_nlp_id INT,
    note_id INT,
    section_concept_id INT,
    snippet STRING,
    offset STRING,
    lexical_variant STRING,
    note_nlp_concept_id INT,
    nlp_system STRING,
    nlp_date DATE,
    nlp_datetime TIMESTAMP,
    term_exists STRING,
    term_temporal STRING,
    term_modifiers STRING,
    note_nlp_source_concept_id INT
  ) USING DELTA;
--ZORDER BY (person_id)
  CREATE
  OR REPLACE TABLE observation (
    observation_id INT,
    person_id INT,
    observation_concept_id INT,
    observation_date DATE,
    observation_datetime TIMESTAMP,
    observation_type_concept_id INT,
    value_as_number DOUBLE,
    value_as_string STRING,
    value_as_concept_id INT,
    qualifier_concept_id INT,
    unit_concept_id INT,
    provider_id INT,
    visit_occurrence_id INT,
    visit_detail_id INT,
    observation_source_value STRING,
    observation_source_concept_id INT,
    unit_source_value STRING,
    qualifier_source_value STRING,
    observation_event_id INT,
    obs_event_field_concept_id INT,
    value_as_datetime TIMESTAMP
  ) USING DELTA;
CREATE
  OR REPLACE TABLE survey_conduct (
    survey_conduct_id INT,
    person_id INT,
    survey_concept_id INT,
    survey_start_date DATE,
    survey_start_datetime TIMESTAMP,
    survey_end_date DATE,
    survey_end_datetime TIMESTAMP,
    provider_id INT,
    assisted_concept_id INT,
    respondent_type_concept_id INT,
    timing_concept_id INT,
    collection_method_concept_id INT,
    assisted_source_value STRING,
    respondent_type_source_value STRING,
    timing_source_value STRING,
    collection_method_source_value STRING,
    survey_source_value STRING,
    survey_source_concept_id INT,
    survey_source_identifier STRING,
    validated_survey_concept_id INT,
    validated_survey_source_value STRING,
    survey_version_number STRING,
    visit_occurrence_id INT,
    visit_detail_id INT,
    response_visit_occurrence_id INT
  ) USING DELTA;
CREATE
  OR REPLACE TABLE fact_relationship (
    domain_concept_id_1 INT,
    fact_id_1 INT,
    domain_concept_id_2 INT,
    fact_id_2 INT,
    relationship_concept_id INT
  ) USING DELTA;
CREATE
  OR REPLACE TABLE location (
    location_id INT,
    address_1 STRING,
    address_2 STRING,
    city STRING,
    state STRING,
    zip STRING,
    county STRING,
    country STRING,
    location_source_value STRING,
    latitude DOUBLE,
    INTitude DOUBLE
  ) USING DELTA;
CREATE
  OR REPLACE TABLE location_history (
    location_history_id INT,
    location_id INT,
    relationship_type_concept_id INT,
    domain_id STRING,
    entity_id INT,
    start_date DATE,
    end_date DATE
  ) USING DELTA;
CREATE
  OR REPLACE TABLE care_site (
    care_site_id INT,
    care_site_name STRING,
    place_of_service_concept_id INT,
    location_id INT,
    care_site_source_value STRING,
    place_of_service_source_value STRING
  ) USING DELTA;
CREATE
  OR REPLACE TABLE provider (
    provider_id INT,
    provider_name STRING,
    npi STRING,
    dea STRING,
    specialty_concept_id INT,
    care_site_id INT,
    year_of_birth INT,
    gender_concept_id INT,
    provider_source_value STRING,
    specialty_source_value STRING,
    specialty_source_concept_id INT,
    gender_source_value STRING,
    gender_source_concept_id INT
  ) USING DELTA;
--ZORDER BY (person_id)
  CREATE
  OR REPLACE TABLE payer_plan_period (
    payer_plan_period_id INT,
    person_id INT,
    contract_person_id INT,
    payer_plan_period_start_date DATE,
    payer_plan_period_end_date date,
    payer_concept_id INT,
    plan_concept_id INT,
    contract_concept_id INT,
    sponsor_concept_id INT,
    stop_reason_concept_id INT,
    payer_source_value STRING,
    payer_source_concept_id INT,
    plan_source_value STRING,
    plan_source_concept_id INT,
    contract_source_value STRING,
    contract_source_concept_id INT,
    sponsor_source_value STRING,
    sponsor_source_concept_id INT,
    family_source_value STRING,
    stop_reason_source_value STRING,
    stop_reason_source_concept_id INT
  ) USING DELTA;
CREATE
  OR REPLACE TABLE cost (
    cost_id INT,
    person_id INT,
    cost_event_id INT,
    cost_event_field_concept_id INT,
    cost_concept_id INT,
    cost_type_concept_id INT,
    currency_concept_id INT,
    cost DOUBLE,
    incurred_date DATE,
    billed_date DATE,
    paid_date DATE,
    revenue_code_concept_id INT,
    drg_concept_id INT,
    cost_source_value STRING,
    cost_source_concept_id INT,
    revenue_code_source_value STRING,
    drg_source_value STRING,
    payer_plan_period_id INT
  ) USING DELTA;
--ZORDER BY (person_id)
  CREATE
  OR REPLACE TABLE drug_era (
    drug_era_id INT,
    person_id INT,
    drug_concept_id INT,
    drug_era_start_datetime TIMESTAMP,
    drug_era_end_datetime TIMESTAMP,
    drug_exposure_count INT,
    gap_days INT
  ) USING DELTA;
--ZORDER BY (person_id)
  CREATE
  OR REPLACE TABLE dose_era (
    dose_era_id INT,
    person_id INT,
    drug_concept_id INT,
    unit_concept_id INT,
    dose_value DOUBLE,
    dose_era_start_datetime TIMESTAMP,
    dose_era_end_datetime TIMESTAMP
  ) USING DELTA;
--ZORDER BY (person_id)
  CREATE
  OR REPLACE TABLE condition_era (
    condition_era_id INT,
    person_id INT,
    condition_concept_id INT,
    condition_era_start_datetime TIMESTAMP,
    condition_era_end_datetime TIMESTAMP,
    condition_occurrence_count INT
  ) USING DELTA;
INSERT INTO
  metadata
VALUES
  (
    0,
    0,
    'OHDSI OMOP CDM Version',
    '6.0.0',
    0,
    CURRENT_DATE,
    CURRENT_TIMESTAMP
  );

-- COMMAND ----------

-- DBTITLE 1,Create OMOP 6.0.0 Result Tables
--ZORDER BY (subject_id)
CREATE
OR REPLACE TABLE cohort (
  cohort_definition_id INT,
  subject_id INT,
  cohort_start_date DATE,
  cohort_end_date DATE
) USING DELTA;
CREATE
OR REPLACE TABLE cohort_definition (
  cohort_definition_id INT,
  cohort_definition_name STRING,
  cohort_definition_description STRING,
  definition_type_concept_id INT,
  cohort_definition_syntax STRING,
  subject_concept_id INT,
  cohort_initiation_date DATE
) USING DELTA;

-- COMMAND ----------

-- DBTITLE 1,Check OMOP 6.0.0 Metadata Version
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
