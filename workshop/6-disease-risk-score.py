# Databricks notebook source
# MAGIC %md
# MAGIC # Patient risk scoring using longitudinal EHR <img src="https://databricks.com/wp-content/themes/databricks/assets/images/header_logo_2x.png" alt="logo" width="150"/> 
# MAGIC 
# MAGIC In this notebook, we use longitudinal synthetic health records, to asses an individual's risk for a given condition. To accomplish that, we use demographic features (such as age, ethnicity and gender), and also a patient's history of comorbidities extracted from EHR data. This problem is framed as a binary classification problem in which classes correspond to being diganosed with the target condition or not (1/0) within a given time frame `t`, and covariates (features) are demographic features and a patient's history of previouse comorbidities. 
# MAGIC 
# MAGIC Users of the notebook can choose the condition to model (default to drug overdose), a window of time to take into account for looking into patient's history (default to 90 days) and the max number of comorbidities to take into account.
# MAGIC 
# MAGIC To train the model we directly use data stored in our bronze delta tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Data preparation
# MAGIC To create the training dataset, we need to extract a dataset of qualified patients.

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
import mlflow

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Specify paths and parameters

# COMMAND ----------

dbutils.widgets.text('condition', 'drug overdose', 'Condition to model')
dbutils.widgets.text('num_conditions', '10', '# of comorbidities to include')
dbutils.widgets.text('num_days', '90', '# of days to use')
dbutils.widgets.text('num_days_future', '10', '# of days in future for forecasting')

# COMMAND ----------

condition=dbutils.widgets.get('condition')
num_conditions=int(dbutils.widgets.get('num_conditions'))
num_days=int(dbutils.widgets.get('num_days'))
num_days_future=int(dbutils.widgets.get('num_days_future'))

# COMMAND ----------

# MAGIC %run ./params

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 1.2 Load Tables

# COMMAND ----------

patient_features=['Id','BIRTHDATE','RACE','GENDER']

# COMMAND ----------

patients = spark.read.format("delta").load(delta_root_path + '/bronze/patients').select(patient_features).withColumnRenamed('Id','PATIENT_ID')
encounters = spark.read.format("delta").load(delta_root_path + '/bronze/encounters')
# organizations = spark.read.format("delta").load(delta_root_path + '/bronze/organizations')

# COMMAND ----------

# MAGIC %md
# MAGIC Create a dataframe of all patient encounteres

# COMMAND ----------

display(patients)

# COMMAND ----------

patient_encounters = (
  encounters 
  .join(patients,encounters['PATIENT']==patients['PATIENT_ID'])
)
display(patient_encounters.filter('REASONDESCRIPTION IS NOT NULL').limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 1.3 Create a list of patients to include

# COMMAND ----------

all_patients=patient_encounters.select('PATIENT').dropDuplicates()
print(all_patients.count())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Get the list of patients with the target condition (cases) and those without the condition (controls) and combine the two lists.

# COMMAND ----------

positive_patients = (
  patient_encounters
  .select('PATIENT')
  .where(lower("REASONDESCRIPTION").like("%{}%".format(condition)))
  .dropDuplicates()
  .withColumn('is_positive',lit(True))
)

negative_patients = (
  all_patients
  .join(positive_patients,on=['PATIENT'],how='left_anti')
  .limit(positive_patients.count())
  .withColumn('is_positive',lit(False))
)

patients_to_study = positive_patients.union(negative_patients)
print(positive_patients.count(),negative_patients.count(),patients_to_study.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Now we limit encounters to patients under study. These patients have been selected to ensure we have a balanced set of cases and controls for our model training:

# COMMAND ----------

qualified_patient_encounters_df = (
  patient_encounters
  .join(patients_to_study,on=['PATIENT'])
  .filter("DESCRIPTION is not NUll")
)
print(qualified_patient_encounters_df.count())

# COMMAND ----------

# DBTITLE 0,distribution of age by gender
display(qualified_patient_encounters_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 2. Feature Engineering and pre-processing
# MAGIC Now we want to add features representing patient's encounter history to the training data. First we need to identify most common comorbidities:

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1. Identify comorbidities based on the dataset

# COMMAND ----------

comorbid_conditions = (
  positive_patients.join(patient_encounters, ['PATIENT'])
  .where(col('REASONDESCRIPTION').isNotNull())
  .dropDuplicates(['PATIENT', 'REASONDESCRIPTION'])
  .groupBy('REASONDESCRIPTION').count()
  .orderBy('count', ascending=False)
  .limit(num_conditions)
)

display(comorbid_conditions)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2. Define auxiliary functions
# MAGIC Next, we define a function that for every comborbid condition adds a binary feature indicating whether a given encounter was realted to the that condition.

# COMMAND ----------

def add_comorbidities(qualified_patient_encounters_df,comorbidity_list):
  
  output_df = qualified_patient_encounters_df
  
  idx = 0
  for comorbidity in comorbidity_list:
      output_df = (
        output_df
        .withColumn("comorbidity_%d" % idx, (output_df['REASONDESCRIPTION'].like('%' + comorbidity['REASONDESCRIPTION'] + '%')).cast('int'))
        .withColumn("comorbidity_%d"  % idx,coalesce(col("comorbidity_%d" % idx),lit(0))) # replacing null values with 0
        .cache()
      )
      idx += 1
  return(output_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Corresponding to each comorbidity we add a column that indicates how many times the condition of interest has been observed in the past, i.e.
# MAGIC 
# MAGIC $$X_t^{(c)} = \sum_{t-w \leq i < t} x_{i,c}$$
# MAGIC 
# MAGIC in which `$$x_{i,c}$$` is an indicator variable which is equal `1` if patient is diagnosed with condition `c` at time `t` and zero otherwise.

# COMMAND ----------

def add_recent_encounters(encounter_features):
  
  lowest_date = (
      encounter_features
      .select('START')
      .orderBy('START')
      .limit(1)
      .withColumnRenamed('START', 'EARLIEST_TIME')
    )
  
  output_df = (
       encounter_features
      .crossJoin(lowest_date)
      .withColumn("day", datediff(col('START'), col('EARLIEST_TIME')))
      .withColumn("patient_age", datediff(col('START'), col('BIRTHDATE')))
    )
  
  w = (
    Window.orderBy(output_df['day'])
    .partitionBy(output_df['PATIENT'])
    .rangeBetween(-int(num_days), -1)
  )
  
  for comorbidity_idx in range(num_conditions):
      col_name = "recent_%d" % comorbidity_idx
      
      output_df = (
        output_df
        .withColumn(col_name, sum(col("comorbidity_%d" % comorbidity_idx)).over(w))
        .withColumn(col_name,coalesce(col(col_name),lit(0)))
      )
  
  return(output_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we need to add the labels, indicating which encounter falls within a given window of time (in the future) that would result the patient being diagnosed with the condition of interest. 

# COMMAND ----------

def add_outcome(encounter_features,num_days_future):

  w = (
      Window.orderBy(encounter_features['day'])
      .partitionBy(encounter_features['PATIENT'])
      .rangeBetween(0,num_days_future)
    )
  
  output_df = (
    encounter_features
    .withColumn('outcome', max(col("comorbidity_0")).over(w))
    .withColumn('outcome',coalesce(col('outcome'),lit(0)))
  )
  
  return(output_df)

# COMMAND ----------

def modify_features(encounter_features):
  return(
    encounter_features
    .withColumn('START_YEAR',year(col('START')))
    .withColumn('START_MONTH',month(col('START')))
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3. Transform input dataset 
# MAGIC Now that we have defined functions to add required features and the label, we proceed to add these features

# COMMAND ----------

comorbidity_list=comorbid_conditions.collect()
encounter_features=add_comorbidities(qualified_patient_encounters_df, comorbidity_list)
encounter_features=add_recent_encounters(encounter_features)
encounter_features=modify_features(encounter_features)
encounter_features=add_outcome(encounter_features,num_days_future)

# COMMAND ----------

display(encounter_features)

# COMMAND ----------

# MAGIC %md
# MAGIC Using mlflow tracking API we log notebook level parameters, which automatically also starts a new experiment run

# COMMAND ----------

mlflow.log_params({'condition':condition,'num_conditions':num_conditions,'num_days':num_days,'num_days_future':num_days_future})

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4. Add experiment metadata and store to delta
# MAGIC Now we write these features into a feature store within Delta Lake. To ensure reproducibility, we add mlflow experiment id and the runid as a column to the the feature store. The advantage of this approach is that we receive more data, we can add new features to the featurestore that can be re-used to referred to in the future.

# COMMAND ----------

run=mlflow.active_run()
(
  encounter_features
  .withColumn('mlflow_experiment_id',lit(run.info.experiment_id))
  .withColumn('mlflow_run_id',lit(run.info.run_id))
  .write.format('delta').mode('overWrite').save(delta_root_path+'/encounter_features')
)
# mlflow.end_run()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Model Training

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1. Data QC and Preprocessing
# MAGIC Before we move ahead with the training task, we take a look at the data to see how different labels are distributed among classes. In many applications of binary classification, one class can be rare, for example in disease prediction. This class imbalance will have a negative impact on the learning process. During the estimation process, the model tends to focus on the majority class at the expense of rare events. Moreover, the evaluation process is also compromised. For example, in an imbalance dataset with 0/1 labels distributed as 95% and %5 respectively,  a model that always predicts 0, would have an accuracy of 95%. If the labels are imbalanced, then we need to apply [one of the common techniques](https://www.kdnuggets.com/2017/06/7-techniques-handle-imbalanced-data.html) for correcting for imbalanced data.

# COMMAND ----------

num_conditions=int(dbutils.widgets.get('num_conditions'))

# COMMAND ----------



input_dataset_df = spark.read.format('delta').load(delta_root_path+'/encounter_features')

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's take a look at the distribution of lables 

# COMMAND ----------

display(input_dataset_df.groupBy('outcome').count())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We note that classes are imbalanced, so for downstream training we need to adjust the distribution of classes. Since we have enough data we choose to downsample the `0` class.

# COMMAND ----------

df1 = input_dataset_df.filter('outcome==1')
n_df1=df1.count()
df2 = input_dataset_df.filter('outcome==0').sample(False,0.9).limit(n_df1)
dataset_df = df1.union(df2).sample(False,1.0)
display(dataset_df.groupBy('outcome').count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2. Classification
# MAGIC Now that we have the dataset in a form that can be used for training, we proceed with training a binary classifer to predict the risk of a patient for the target condition based on demography and medical hostrory. To do so, we chose [logistic regression implemented in spark](https://spark.apache.org/docs/latest/ml-classification-regression.html#binomial-logistic-regression). 
# MAGIC <!-- <img src="https://secure.meetupstatic.com/photos/event/c/1/7/6/600_472189526.jpeg" width=100> -->

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.2.1. Define features to include

# COMMAND ----------

encounter_cols=['ENCOUNTERCLASS','START_YEAR']
patient_cols = ['RACE','GENDER','patient_age']
comorbidty_cols = ['recent_%s'%idx for idx in range(0,num_conditions)] + ['comorbidity_%s'%idx for idx in range(0,num_conditions)]
selected_features = encounter_cols+patient_cols+comorbidty_cols

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.2.2. Pre-processing stages
# MAGIC 
# MAGIC For logistic regression, all covariates are assumed to be numeric. Therefore we need to transform categorical features into numeric features. This is done in two stages:
# MAGIC 1. Category Indexing: This is basically assigning a numeric value to each category from {0, 1, 2, ...numCategories-1}. This introduces an implicit ordering among your categories, and is more suitable for ordinal variables (eg: Poor: 0, Average: 1, Good: 2)
# MAGIC 2. One-Hot Encoding: This converts categories into binary vectors with at most one nonzero value (eg: (Blue: [1, 0]), (Green: [0, 1]), (Red: [0, 0]))
# MAGIC In the cell below we index each categorical column using the StringIndexer, and then convert the indexed categories into one-hot encoded variables. The resulting output has the binary vectors appended to the end of each row. 

# COMMAND ----------

import pyspark
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from distutils.version import LooseVersion
import mlflow
def create_stages_pipeline(categoricalColumns= ['ENCOUNTERCLASS','RACE','GENDER'],
                            numericCols = ['patient_age','recent_0','recent_1','recent_2','recent_3','recent_4','recent_5','recent_6',\
                                             'recent_7','recent_8','recent_9','comorbidity_0','comorbidity_1','comorbidity_2','comorbidity_3',\
                                           'comorbidity_4','comorbidity_5','comorbidity_6','comorbidity_7','comorbidity_8','comorbidity_9'],
                            target_column_name='outcome'):
  mlflow.spark.autolog()
  stages = [] # stages in our Pipeline
  for categoricalCol in categoricalColumns:
      # Category Indexing with StringIndexer
      stringIndexer = StringIndexer(inputCol=categoricalCol, outputCol=categoricalCol + "Index")
      # Use OneHotEncoder to convert categorical variables into binary SparseVectors
      if LooseVersion(pyspark.__version__) < LooseVersion("3.0"):
          from pyspark.ml.feature import OneHotEncoderEstimator
          encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
      else:
          from pyspark.ml.feature import OneHotEncoder
          encoder = OneHotEncoder(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
      # Add stages.  These are not run here, but will run all at once later on.
      stages += [stringIndexer, encoder]
  # Convert outcome into label indices using the StringIndexer
  if target_column_name:
    label_stringIdx = StringIndexer(inputCol=target_column_name, outputCol="label")
    stages += [label_stringIdx]
  # Transform all features into a vector using VectorAssembler
  assemblerInputs = [c + "classVec" for c in categoricalColumns] + numericCols
  assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
  stages += [assembler]
  return(stages)
  

# COMMAND ----------

# MAGIC %md
# MAGIC Now we create preprocessing pipeline and fit it to the dataset.

# COMMAND ----------

stages=create_stages_pipeline()
partial_pipeline = Pipeline().setStages(stages)
pipeline_model = partial_pipeline.fit(dataset_df)
prepped_data_df = pipeline_model.transform(dataset_df)

# COMMAND ----------

# Keep relevant columns
cols = dataset_df.columns
selectedcols = ["label", "features"] + cols
dataset = prepped_data_df.select(selectedcols)
display(dataset)

# COMMAND ----------

# MAGIC %md
# MAGIC Next we randomly split data into training, test and validation sets. set seed for reproducibility

# COMMAND ----------

(trainingData, testData, validationData) = dataset.randomSplit([0.6, 0.3, 0.1], seed=43)
print(trainingData.count())
print(testData.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.2.3. Evaluate the model's accuracy

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression
from pyspark.mllib.evaluation import BinaryClassificationMetrics
# # Create initial LogisticRegression model
lr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10, regParam=0.3, elasticNetParam=0.8)
# Train model with Training Data
lrModel = lr.fit(trainingData)
print("The model accuracy, evaluated on test data is %f" % lrModel.evaluate(testData).accuracy)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.2.4. Train a calssifier and log the model using `mlflow`
# MAGIC Now using `mlflow`'s model API, we log our trained model 

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression

stages=create_stages_pipeline()
lr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10, regParam=0.3, elasticNetParam=0.8)
stages+=[lr]
pipeline = Pipeline().setStages(stages)
pipeline_model = pipeline.fit(dataset_df)
# pipeline_model.transform(input_dataset_df)
# prepped_data_df = pipeline_model.transform(dataset_df)


# COMMAND ----------

import mlflow.spark
model_name='patient-risk-lr'
mlflow.spark.log_model(pipeline_model,model_name)
model_uri = "runs:/{run_id}/{artifact_path}".format(run_id=mlflow.active_run().info.run_id, artifact_path=model_name)
print(model_uri)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Apply the trained model to estimate risk

# COMMAND ----------

model=mlflow.spark.load_model(model_uri)

# COMMAND ----------

scored=model.transform(dataset_df)

# COMMAND ----------

from pyspark.sql.functions import udf,col
from pyspark.sql.types import FloatType
display(scored.select('RACE','GENDER','patient_age','prediction',udf(lambda v:float(v[1]),FloatType())('probability').alias('prob')))

# COMMAND ----------

# MAGIC %md
# MAGIC Copyright / License info of the notebook. Copyright Databricks, Inc. [2021].  The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC 
# MAGIC |Library Name|Library License|Library License URL|Library Source URL| 
# MAGIC | :-: | :-:| :-: | :-:|
# MAGIC |Spark|Apache-2.0 License | https://github.com/apache/spark/blob/master/LICENSE | https://github.com/apache/spark  |
# MAGIC |mlflow|Apache License 2.0|Apache License 2.0|https://github.com/mlflow/mlflow|

# COMMAND ----------


