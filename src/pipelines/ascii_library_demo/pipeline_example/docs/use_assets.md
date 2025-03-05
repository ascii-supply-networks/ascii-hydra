# Building and Running Multi-Cloud Assets in Dagster
This guide describes how to create reusable Spark-based assets and wire them up into a Dagster job pipeline using ASCII-Hydra. It covers the basic steps—from defining an asset using a factory function to creating a job that orchestrates the assets’ execution.

## Prerequisites

Before you begin, make sure that you have:
- Pixi installed and added to your PATH.
- Git, Docker, and Make installed on your system.
- This example uses PySpark so if you use local deployment on windows you will need Hadoop.

### Credentials:
- AWS credentials for EMR and S3 (set in the environment variables as `ASCII_AWS_ACCESS_KEY_ID`, `ASCII_AWS_SECRET_ACCESS_KEY`).
- Databricks credentials (if you want to use DBR you need to set: `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`).

### Specific environment variables:

Set variables such as  `SPARK_PIPES_ENGINE` default is pyspark, `SPARK_EXECUTION_MODE` this is use in private ASCII development to switch between subsamples of data or full samples, and `DAGSTER_HOME` to control execution behavior. For mor specific behaviour please check the Readme

## Creating Spark-Based Assets using Asset Factory Function
Assets in ASCII-Hydra represent data artifacts produced by your pipelines that can be run in Pyspark, AWS EMR, or Databricks. 
Key Concepts:
-  External Script File:
Each asset is defined by an external Python script (e.g., `step_1.py`) that implements a Spark job. This script should contain an `execute_business_logic()` method that accepts a Dagster context, a Spark session, and other parameters.

- Configuration Parameters:
    - __name:__ A unique identifier for the asset.
    - __key_prefix:__ A namespace for asset grouping.
    - __group_name:__ A grouping identifier (useful for organizing assets).
    - __external_script_file:__ The file path to the Spark job script.
    - __local_spark_config:__ Local Spark configuration for testing. It's strongly encouraged to modify it to your local development
    - __databricks_cluster_config/emr_job_config:__ (Optional) Cloud-specific cluster configurations.
    - __libraries_config:__ A list of libraries (e.g. Python wheels or PyPI packages) that the job needs.
    - __deps:__ Dependencies on other assets. This parameter allows you to chain multiple assets together.

## Step-by-Step Code Walkthrough
Below is a walkthrough of the example code provided.
### Asset: Step_1
The first asset (step1) performs the following tasks:
- Generates Random Data: Creates a DataFrame with random values.
- Writes Data to S3: Saves the DataFrame as a Parquet file to an S3 bucket.
- Reports Materialization: Records metadata (start and end times) using Dagster’s context.report_asset_materialization().
### Asset: Step_2
The second asset (step2) reads the data produced by Step_1:

- Reads Parquet Data from S3: Loads the data from the S3 bucket.
- Performs Transformations: Adds a column (text_length), computes averages, and counts distinct values.
- Reports Materialization: Reports computed statistics along with timing metadata.

### Defining the Pipeline Job
The two assets are chained into a single Dagster job using define_asset_job(). A top-level Definitions object loads the assets and resources (such as the spark_pipes_client), tying the entire pipeline together. 

## Running the Pipeline

1) After setting up your environment (as described in the private readme), you can launch the Dagster UI and run your pipeline:
`pixi run start` or `make start`
2) Open `http://localhost:3000` to view the pipeline, inspect asset lineage, and monitor execution logs.
3) Trigger the pipeline from the UI