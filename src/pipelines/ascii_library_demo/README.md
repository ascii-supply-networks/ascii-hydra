# ASCII hydra

## showcase

The ASCII Hydra project demonstrates a cost-efficient alternative to being locked into specific cloud platforms like Databricks. This examples provide out of the box way of creating assets using either local pyspark, Databricks or AWS EMR.

### preprequisites

- pixi `curl -fsSL https://pixi.sh/install.sh | bash`
- credentials for:
    - AWS EMR
        - `ASCII_AWS_ACCESS_KEY_ID`: Your AWS Access Key ID for EMR.
        - `ASCII_AWS_SECRET_ACCESS_KEY`: Your AWS Secret Access Key for EMR.  
    - Databricks
        - `DATABRICKS_HOST`: The Databricks host URL.
        - `DATABRICKS_CLIENT_ID`: Your Databricks Client ID.
        - `DATABRICKS_CLIENT_SECRET`: Your Databricks Client Secret.
- set up a couple of environment variables:
    - `SPARK_PIPES_ENGINE`: Specifies the engine used for Spark Pipes (valid options: `databricks`, `emr`, or `pyspark`).
    - `SPARK_EXECUTION_MODE`: Defines the mode of execution for data (valid options: `small_dev_sample_local`, `small_dev_sample_s3`, or `full`).
    - `DAGSTER_HOME`: Path to the Dagster home directory where Dagster-related configurations and metadata are stored.

### Explanation of `SPARK_EXECUTION_MODE`

The `SPARK_EXECUTION_MODE` environment variable controls the scope and source of the data used during the pipeline execution. It will be transformed on the class `ExecutionMode` and it's thought to be use as a flag at the external script level:

    `small_dev_sample_local`:
    This mode should use a small, locally stored sample dataset, ideal for fast development and testing on your local machine.

    `small_dev_sample_s3`:
    This mode should use a small sample dataset stored on Amazon S3, allowing you to test the pipeline in a cloud environment with minimal data.

    `full`:
    This mode should processes the full dataset stored on Amazon S3, intended for complete runs and production-level processing.

### Creation of environment

To create the environment execute the following commands:

```bash
pixi run start

pixi run -e ci fmt
pixi run -e ci lint
```

then go to http://localhost:3000 for the dagster UI

## explanation

See [https://georgheiler.com/2024/06/21/cost-efficient-alternative-to-databricks-lock-in/](https://georgheiler.com/2024/06/21/cost-efficient-alternative-to-databricks-lock-in/).
