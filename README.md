# Using Dataproc Serverless/Batch for JAR file execution
This sample covers:
* CI/CD pipeline using Cloud Build to automate the conversion of Scala Spark code into a JAR file
* Executing the JAR in Dataproc Batch (serverless) environment

## Sample usecase
This is a sample implementation of Product Order system. Data is processed in various stages (bronze, silver & gold).

### Google Cloud Services used

#### 1. Cloud Build

Used for building JAR file. The code base contains cloudbuild.yaml which will execute when a code is pushed in the repository. 

*cloudbuild.yaml* contains instructions for:
* **code compilation and JAR creation**: it uses docker/container image 'sbtscala/scala-sbt:eclipse-temurin-17.0.4_1.7.1_3.2.0' for compiling the code and creating a JAR
* **copying JAR to GCS bucket**: it uses the docker/container image 'google/cloud-sdk:420.0.0-slim' for copying the JAR to desired GCS location.


#### 2. Cloud Storage

* Raw data: GCS bucket <RAW_FOLDER_PATH> contain raw CSV files.

* Delta tables (bronze/silve/gold): GCS bucket <DELTA_FOLDER_PATH> contain files for delta tables created during all the layers.

#### 3. Dataproc serverless
Spark jobs for creating medallion architecture, data transformations at every layer and meaningful insights are getting run on Dataproc serverless.

#### 4. BigQuery

Gold layer tables are loaded in BigQuery for analysis. 


## Sample code/folder structure

```bash
.
├── DAOs                    # DAOs files to provide data operations for different formats
│   ├── BigQueryDAO.scala
│   ├── CsvDAO.scala 
│   ├── DeltaLakeDAO.scala 
├── DAGs                    # DAG files which will run in airflow
│   ├── product_order_pipeline.py 
├── src                     # Source files
│   ├── setup.scala
│   ├── bronze.scala 
│   ├── silver.scala 
│   ├── gold.scala
│   ├── deltaToBigQuery.scala 
│   ├── commonUtilities.scala  
├── project 
│   ├── assembly.sbt         # adds assembly plugin which helps to run the command 'sbt assembly'
│   ├── build.properties         
├── build.sbt                # Specifies the project's settings, dependencies and other build related configurations
├── cloudbuild.yaml          # Defines the build steps and configurations for CI/CD pipeline
└── README.md
```


### Library versions specified in sbt file

* SparkVersion - "3.3.2"
* ScalaVersion - "2.13.11"
* DeltaVersion - "2.3.0"


## Running JAR file

### Step 1: Setting automated JAR build process
* Create a cloud build trigger for the repository:
    - Set the event type 'Push to a branch', configuration type 'Cloud Build configuration(yaml)' and yaml file's location as 'Repository'.

* Create the following substitution variables while setting the trigger:

  * _GCS_JARS_PATH = gs://product_order_scala/jars/product-order.jar    # Add the GCS location where you want to save jar file

  * _LOCAL_JAR_PATH = target/scala-2.13/product-order-assembly-0.1.jar   

* Once the code is pushed in a repository, jar file will be available at specified GCS location within minutes. The status and log files for the cloud build can be seen in cloud build history.
 
### Step 2: Executing JAR file

#### Method 1: Using Dataproc batch UI

Following needs to be passed as argument when submitting dataproc batch job. Make sure the buckets specified in arguments and bigquery dataset with layer name are created before running it- 

PROJECT_ID=original-gasket-405913
TEMP_BQ_BUCKET=gs://temp-bigquery1
RAW_CSV_PATH=gs://product_order_use_case
DELTA_FOLDER_PATH=gs://product_order_scala
RAW_FOLDER_PATH=gs://product_order_use_case
LAYER=gold


#### Method 2: Using CLI

```
gcloud dataproc batches submit spark \
--project original-gasket-405913 \
--region us-central1  \
--class <CLASS_NAME> \
--jars gs://product_order_scala/jars/product-order.jar,gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.13-0.31.1.jar \
--service-account <SERVICE_ACCOUNT> \
--properties spark.jars.packages=io.delta:delta-core_2.13:2.3.0 \
-- PROJECT_ID=original-gasket-405913 TEMP_BQ_BUCKET=gs://temp-bigquery1 RAW_CSV_PATH=gs://product_order_use_case DELTA_FOLDER_PATH=gs://product_order_scala RAW_FOLDER_PATH=gs://product_order_use_case LAYER=gold
```

#### Method 3: Using Airflow
 
 Upload the 'DAGs/product_order_pipeline.py' in DAGs folder of airflow. Trigger or schedule the run accordingly.


 