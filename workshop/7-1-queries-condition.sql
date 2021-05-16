-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Condition 
-- MAGIC ([source](https://github.com/OHDSI/OMOP-Queries/blob/master/md/Condition.md#condition-queries))

-- COMMAND ----------

use OMOP531

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C01: Find condition by concept ID
-- MAGIC Find condition by condition ID is the lookup for obtaining condition or disease concept details associated with a concept identifier. This query is a tool for quick reference for the name, class, level and source vocabulary details associated with a concept identifier, either SNOMED-CT clinical finding or MedDRA. 

-- COMMAND ----------

SELECT 
	  C.concept_id Condition_concept_id, 
	  C.concept_name Condition_concept_name, 
	  C.concept_code Condition_concept_code, 
	  C.concept_class_id Condition_concept_class,
	  C.vocabulary_id Condition_concept_vocab_ID, 
	  V.vocabulary_name Condition_concept_vocab_name, 
	  CASE C.vocabulary_id 
	    WHEN 'SNOMED' THEN CASE lower(C.concept_class_id)   
		  WHEN 'clinical finding' THEN 'Yes' ELSE 'No' END 
		WHEN 'MedDRA' THEN 'Yes'
		ELSE 'No' 
	  END Is_Disease_Concept_flag 
	FROM concept C, vocabulary V 
	WHERE 
	  C.concept_id = 24134 AND -- Neck Pain
	  C.vocabulary_id = V.vocabulary_id AND 
	  current_date BETWEEN valid_start_date AND valid_end_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C02: Find a condition by keyword
-- MAGIC This query enables search of vocabulary entities by keyword. The query does a search of standard concepts names in the CONDITION domain (SNOMED-CT clinical findings and MedDRA concepts) and their synonyms to return all related concepts.
-- MAGIC 
-- MAGIC It does not require prior knowledge of where in the logic of the vocabularies the entity is situated.
-- MAGIC 
-- MAGIC The following is a sample run of the query to run a search of the Condition domain for keyword 'myocardial infarction'. The input parameters are highlighted in blue.

-- COMMAND ----------

SELECT 
	  T.Entity_Concept_Id, 
	  T.Entity_Name, 
	  T.Entity_Code, 
	  T.Entity_Type, 
	  T.Entity_concept_class, 
	  T.Entity_vocabulary_id, 
	  T.Entity_vocabulary_name 
	FROM ( 
	  SELECT 
	    C.concept_id Entity_Concept_Id, 
		C.concept_name Entity_Name, 
		C.CONCEPT_CODE Entity_Code, 
		'Concept' Entity_Type, 
		C.concept_class_id Entity_concept_class, 
		C.vocabulary_id Entity_vocabulary_id, 
		V.vocabulary_name Entity_vocabulary_name, 
		NULL Entity_Mapping_Type, 
		C.valid_start_date, 
		C.valid_end_date 
	  FROM concept C 
	  JOIN vocabulary V ON C.vocabulary_id = V.vocabulary_id 
	  LEFT JOIN concept_synonym S ON C.concept_id = S.concept_id 
	  WHERE 
	    (C.vocabulary_id IN ('SNOMED', 'MedDRA') OR LOWER(C.concept_class_id) = 'clinical finding' ) AND 
		C.concept_class_id IS NOT NULL AND 
		( LOWER(C.concept_name) like 'myocardial infarction' OR -- e.g. myocardial infarction
		  LOWER(S.concept_synonym_name) like '%myocardial infarction%' ) 
	  ) T
	WHERE current_date BETWEEN valid_start_date AND valid_end_date 
	ORDER BY 6,2;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC This is a comprehensive query to find relevant terms in the vocabulary. To constrain, additional clauses can be added to the query. However, it is recommended to do a filtering after the result set is produced to avoid syntactical mistakes.
-- MAGIC 
-- MAGIC The query only returns concepts that are part of the Standard Vocabulary, ie. they have concept level that is not 0. If all concepts are needed, including the non-standard ones, the clause in the query restricting the concept level and concept class can be commented out.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C03: Translate a SNOMED-CT concept into a MedDRA concept
-- MAGIC This query accepts a SNOMED-CT concept ID as input and returns details of the equivalent MedDRA concepts.
-- MAGIC 
-- MAGIC The relationships in the vocabulary associate MedDRA 'Preferred Term' to SNOMED-CT 'clinical findings'. The respective hierarchy for MedDRA and SNOMED-CT can be used to traverse up and down the hierarchy of each of these individual vocabularies.
-- MAGIC 
-- MAGIC Also, not all SNOMED-CT clinical findings are mapped to a MedDRA concept in the vocabulary.
-- MAGIC 
-- MAGIC The following is a sample run of the query to list MedDRA equivalents for SNOMED-CT concept whose concept ID is entered as input.

-- COMMAND ----------

SELECT	D.concept_id Snomed_concept_id,
			D.concept_name Snomed_concept_name,
			D.concept_code Snomed_concept_code,
			D.concept_class_id Snomed_concept_class,
			CR.relationship_id,
			RT.relationship_name,
			A.Concept_id MedDRA_concept_id,
			A.Concept_name MedDRA_concept_name,
			A.Concept_code MedDRA_concept_code,
			A.Concept_class_id MedDRA_concept_class 
	FROM concept_relationship CR, concept A, concept D, relationship RT 
	WHERE CR.relationship_id =  'SNOMED - MedDRA eq'
	AND CR.concept_id_2 = A.concept_id 
	AND CR.concept_id_1 = 312327 
	AND CR.concept_id_1 = D.concept_id 
	AND CR.relationship_id = RT.relationship_id 
	AND current_date BETWEEN CR.valid_start_date 
	AND CR.valid_end_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##C04: Translate a MedDRA concept into a SNOMED-CT concept
-- MAGIC This query accepts a MedDRA concept ID as input and returns details of the equivalent SNOMED-CT concepts. The existing relationships in the vocabulary associate MedDRA 'Preferred Term' to SNOMED-CT 'clinical findings'. The respective hierarchy for MedDRA and SNOMED-CT can be used to traverse up and down the hierarchy of each of these individual vocabularies.

-- COMMAND ----------

SELECT	D.concept_id MedDRA_concept_id,
			D.concept_name MedDRA_concept_name,
			D.concept_code MedDRA_concept_code,
			D.concept_class_id MedDRA_concept_class,
			CR.relationship_id,
			RT.relationship_name,
			A.concept_id Snomed_concept_id,
			A.concept_name Snomed_concept_name,
			A.concept_code Snomed_concept_code,
			A.concept_class_id Snomed_concept_class 
	FROM concept_relationship CR, concept A, concept D, relationship RT 
	WHERE CR.relationship_id = 'MedDRA to SNOMED equivalent (OMOP)'
	AND CR.concept_id_2 = A.concept_id 
	AND CR.concept_id_1 = 35205180 -- 

	AND CR.concept_id_1 = D.concept_id 
	AND CR.relationship_id = RT.relationship_id 
	AND current_date BETWEEN CR.valid_start_date 
	AND CR.valid_end_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C05: Translate a source code to condition concepts
-- MAGIC This query enables to search all Standard SNOMED-CT concepts that are mapped to a condition (disease) source code. It can be used to translate e.g. ICD-9-CM, ICD-10-CM or Read codes to SNOMED-CT.
-- MAGIC 
-- MAGIC Source codes are not unique across different source vocabularies, therefore the source vocabulary ID must also be provided.
-- MAGIC 
-- MAGIC The following source vocabularies have condition/disease codes that map to SNOMED-CT concepts:
-- MAGIC 
-- MAGIC - ICD-9-CM, Vocabulary_id=2
-- MAGIC - Read, Vocabulary_id=17
-- MAGIC - OXMIS, Vocabulary_id=18
-- MAGIC - ICD-10-CM, Vocabulary_id=34
-- MAGIC 
-- MAGIC The following is a sample run of the query to list SNOMED-CT concepts that a set of mapped codes entered as input map to. The sample parameter substitutions are highlighted in blue

-- COMMAND ----------

	SELECT DISTINCT 
	  c1.concept_code, 
	  c1.concept_name, 
	  c1.vocabulary_id source_vocabulary_id, 
	  VS.vocabulary_name source_vocabulary_description, 
	  C1.domain_id, 
	  C2.concept_id target_concept_id, 
	  C2.concept_name target_Concept_Name, 
	  C2.concept_code target_Concept_Code, 
	  C2.concept_class_id target_Concept_Class, 
	  C2.vocabulary_id target_Concept_Vocab_ID, 
	  VT.vocabulary_name target_Concept_Vocab_Name 
	FROM 
	  concept_relationship cr, 
	  concept c1, 
	  concept c2,
	  vocabulary VS, 
	  vocabulary VT 
	WHERE 
	  cr.concept_id_1 = c1.concept_id AND
	  cr.relationship_id = 'Maps to' AND
	  cr.concept_id_2 = c2.concept_id AND
	  c1.vocabulary_id = VS.vocabulary_id AND 
	  c1.domain_id = 'Condition' AND 
	  c2.vocabulary_id = VT.vocabulary_id AND 
	  c1.concept_code IN (
	'070.0'                                           
	) AND c2.vocabulary_id =
	'SNOMED'                                          
	AND
	current_date                                           
	BETWEEN c1.valid_start_date AND c1.valid_end_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C06: Translate a given condition to source codes
-- MAGIC This query allows to search all source codes that are mapped to a SNOMED-CT clinical finding concept. It can be used to translate SNOMED-CT to ICD-9-CM, ICD-10-CM, Read or OXMIS codes.

-- COMMAND ----------

SELECT DISTINCT
	  c1.concept_code,
	  c1.concept_name,
	  c1.vocabulary_id source_vocabulary_id,
	  VS.vocabulary_name source_vocabulary_description,
	  C1.domain_id,
	  C2.concept_id target_concept_id,
	  C2.concept_name target_Concept_Name,
	  C2.concept_code target_Concept_Code,
	  C2.concept_class_id target_Concept_Class,
	  C2.vocabulary_id target_Concept_Vocab_ID,
	  VT.vocabulary_name target_Concept_Vocab_Name
	FROM
	  concept_relationship cr,
	  concept c1,
	  concept c2,
	  vocabulary VS,
	  vocabulary VT
	WHERE
	  cr.concept_id_1 = c1.concept_id AND
	  cr.relationship_id = 'Maps to' AND
	  cr.concept_id_2 = c2.concept_id AND
	  c1.vocabulary_id = VS.vocabulary_id AND
	  c1.domain_id = 'Condition' AND
	  c2.vocabulary_id = VT.vocabulary_id AND
	  c1.concept_id = 312327  --                                            
	  AND c1.vocabulary_id =
	'SNOMED'                                          
	AND
	current_date                                           
	BETWEEN c2.valid_start_date AND c2.valid_end_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C07: Find a pathogen by keyword
-- MAGIC This query enables a search of all pathogens using a keyword as input. The resulting concepts could be used in query C09 to identify diseases caused by a certain pathogen.

-- COMMAND ----------

SELECT 
	  C.concept_id Pathogen_Concept_ID, 
	  C.concept_name Pathogen_Concept_Name, 
	  C.concept_code Pathogen_concept_code, 
	  C.concept_class_id Pathogen_concept_class, 
	  C.standard_concept Pathogen_Standard_Concept, 
	  C.vocabulary_id Pathogen_Concept_Vocab_ID, 
	  V.vocabulary_name Pathogen_Concept_Vocab_Name 
	FROM 
	  concept C, 
	  vocabulary V
	WHERE 
	  LOWER(C.concept_class_id) = 'organism' AND 
	  LOWER(C.concept_name) like
	'%trypanosoma%'                                
	AND C.vocabulary_id = V.vocabulary_id AND
	current_date                                        
	BETWEEN C.valid_start_date AND C.valid_end_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C08: Find a disease causing agent by keyword
-- MAGIC This query enables a search of various agents that can cause disease by keyword as input. Apart from pathogens (see query C07), these agents can be SNOMED-CT concepts of the following classes:
-- MAGIC 
-- MAGIC Pharmaceutical / biologic product
-- MAGIC Physical object
-- MAGIC Special concept
-- MAGIC Event
-- MAGIC Physical force
-- MAGIC Substance
-- MAGIC The resulting concepts could be used in query C09 to identify diseases caused by the agent.

-- COMMAND ----------

SELECT
	  C.concept_id Agent_Concept_ID,
	  C.concept_name Agent_Concept_Name,
	  C.concept_code Agent_concept_code,
	  C.concept_class_id Agent_concept_class,
	  C.standard_concept Agent_Standard_Concept,
	  C.vocabulary_id Agent_Concept_Vocab_ID,
	  V.vocabulary_name Agent_Concept_Vocab_Name
	FROM
	  concept C,
	  vocabulary V
	WHERE
	  LOWER(C.concept_class_id) in ('pharmaceutical / biologic product','physical object',
	                                'special concept','event', 'physical force','substance') AND
	  LOWER(C.concept_name) like '%cancer%'                                  
	AND C.vocabulary_id = V.vocabulary_id AND
	current_date                                        
	BETWEEN C.valid_start_date AND C.valid_end_date;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C09: Find all SNOMED-CT condition concepts that can be caused by a given pathogen or causative agent
-- MAGIC This query accepts a SNOMED-CT pathogen ID as input and returns all conditions caused by the pathogen or disease causing agent identified using queries C07 or C08.

-- COMMAND ----------

SELECT 
	  A.concept_Id Condition_ID, 
	  A.concept_Name Condition_name, 
	  A.concept_Code Condition_code, 
	  A.concept_Class_id Condition_class, 
	  A.vocabulary_id Condition_vocab_ID, 
	  VA.vocabulary_name Condition_vocab_name, 
	  D.concept_Id Causative_agent_ID, 
	  D.concept_Name Causative_agent_Name, 
	  D.concept_Code Causative_agent_Code, 
	  D.concept_Class_id Causative_agent_Class, 
	  D.vocabulary_id Causative_agent_vocab_ID, 
	  VS.vocabulary_name Causative_agent_vocab_name 
	FROM 
	  concept_relationship CR, 
	  concept A, 
	  concept D, 
	  vocabulary VA, 
	  vocabulary VS
	WHERE 
	  CR.relationship_ID = 'Has causative agent' AND 
	  CR.concept_id_1 = A.concept_id AND 
	  A.vocabulary_id = VA.vocabulary_id AND 
	  CR.concept_id_2 = D.concept_id AND 
	  D.concept_id = 4248851   -- 4248851                                             
	  AND D.vocabulary_id = VS.vocabulary_id AND 
	current_date                                             
	  BETWEEN CR.valid_start_date AND CR.valid_end_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C10: Find an anatomical site by keyword
-- MAGIC This query enables a search of all anatomical sites using a keyword entered as input. The resulting concepts could be used in query C11 to identify diseases occurring at a certain anatomical site.

-- COMMAND ----------

SELECT 
	  C.concept_id Anatomical_site_ID, 
	  C.concept_name Anatomical_site_Name, 
	  C.concept_code Anatomical_site_Code, 
	  C.concept_class_id Anatomical_site_Class, 
	  C.standard_concept Anatomical_standard_concept, 
	  C.vocabulary_id Anatomical_site_Vocab_ID, 
	  V.vocabulary_name Anatomical_site_Vocab_Name 
	FROM 
	  concept C, 
	  vocabulary V 
	WHERE 
	  LOWER(C.concept_class_id) = 'body structure' AND  --  body structure
	  LOWER(C.concept_name) like '%epiglottis%' -- epiglottis                                  
	AND C.vocabulary_id = V.vocabulary_id AND
	current_date                                          
	BETWEEN C.valid_start_date AND C.valid_end_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C11: Find all SNOMED-CT condition concepts that are occurring at an anatomical site
-- MAGIC This query accepts a SNOMED-CT body structure ID as input and returns all conditions occurring in the anatomical site, which can be identified using query C10. Input:

-- COMMAND ----------

SELECT
	  A.concept_Id Condition_ID,
	  A.concept_Name Condition_name,
	  A.concept_Code Condition_code,
	  A.concept_Class_id Condition_class,
	  A.vocabulary_id Condition_vocab_ID,
	  VA.vocabulary_name Condition_vocab_name,
	  D.concept_Id Anatomical_site_ID,
	  D.concept_Name Anatomical_site_Name,
	  D.concept_Code Anatomical_site_Code,
	  D.concept_Class_id Anatomical_site_Class,
	  D.vocabulary_id Anatomical_site_vocab_ID,
	  VS.vocabulary_name Anatomical_site_vocab_name
	FROM
	  concept_relationship CR,
	  concept A,
	  concept D,
	  vocabulary VA,
	  vocabulary VS
	WHERE
	  CR.relationship_ID = 'Has finding site' AND
	  CR.concept_id_1 = A.concept_id AND
	  A.vocabulary_id = VA.vocabulary_id AND
	  CR.concept_id_2 = D.concept_id AND
	  D.concept_id = 4103720 -- 
	  AND D.vocabulary_id = VS.vocabulary_id AND
      current_date BETWEEN CR.valid_start_date AND CR.valid_end_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Copyright / License info of the notebook. Copyright Databricks, Inc. [2021].  The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
-- MAGIC 
-- MAGIC |Library Name|Library License|Library License URL|Library Source URL| 
-- MAGIC | :-: | :-:| :-: | :-:|
-- MAGIC | Spark|Apache-2.0 License | https://github.com/apache/spark/blob/master/LICENSE | https://github.com/apache/spark  |
-- MAGIC |OHDSI/OMOP-Queries|||https://github.com/OHDSI/OMOP-Queries|

-- COMMAND ----------


