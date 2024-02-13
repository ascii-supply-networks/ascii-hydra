import os

from dagster_pyspark import lazy_pyspark_resource


def get_pyspark_config(
    url: str = None,
    AWS_ACCESS_KEY_ID: str = "ASCII_AWS_ACCESS_KEY_ID",
    AWS_SECRET_ACCESS_KEY: str = "ASCII_AWS_SECRET_ACCESS_KEY",
):
    return lazy_pyspark_resource.configured(
        dev_spark_config(url, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    )


def dev_spark_config(
    url: str = None,
    AWS_ACCESS_KEY_ID: str = "ASCII_AWS_ACCESS_KEY_ID",
    AWS_SECRET_ACCESS_KEY: str = "ASCII_AWS_SECRET_ACCESS_KEY",
):
    try:
        key = os.environ[AWS_ACCESS_KEY_ID]
        secret = os.environ[AWS_SECRET_ACCESS_KEY]
    except KeyError:
        print(
            f"Please set {AWS_ACCESS_KEY_ID} and {AWS_SECRET_ACCESS_KEY} environment variables - using invalid default values instead"
        )
        key = "NO_KEY_PROVIDED"
        secret = "NO_SECRET_PROVIDED"
    results = {
        "spark_conf": {
            "spark.master": "local[7]",
            "spark.driver.memory": "16G",
            "spark.sql.session.timeZone": "UTC",
            "spark.driver.extraJavaOptions": "-Duser.timezone=UTC",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.skewedJoin.enabled": "true",
            "spark.sql.cbo.enabled": "true",
            "spark.sql.cbo.joinReorder.enabled": "true",
            "spark.sql.cbo.starSchemaDetection": "true",
            "spark.sql.autoBroadcastJoinThreshold": "500MB",
            "spark.driver.maxResultSize": "5G",
            "spark.sql.shuffle.partitions": "50",
            "spark.sql.statistics.histogram.enabled": "true",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            # TODO: consider fine tuning https://spark.apache.org/docs/latest/cloud-integration.html
            "spark.hadoop.fs.s3a.committer.name": "directory",
            # "spark.sql.sources.commitProtocolClass": "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol"
            "spark.sql.parquet.output.committer.class": "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter",
            "spark.hadoop.fs.s3a.fast.upload": "true",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            # "spark.hadoop.fs.s3a.endpoint": url,
            "spark.hadoop.fs.s3a.buffer.dir": "/tmp",
            "spark.hadoop.fs.s3a.fast.upload.buffer": "bytebuffer",
            "spark.hadoop.fs.s3a.fast.upload.active.blocks": "4",
            # "spark.databricks.delta.schema.autoMerge.enabled": "true",
            # "spark.sql.parquet.mergeSchema": "true",
            "spark.sql.parquet.compression.codec": "gzip",
            "spark.hadoop.fs.s3a.access.key": key,
            "spark.hadoop.fs.s3a.secret.key": secret,
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.jars.packages": "io.delta:delta-spark_2.12:3.1.0,org.postgresql:postgresql:42.7.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-hadoop-cloud_2.12:3.5.0",
        }
    }
    if url:
        results["spark_conf"]["spark.hadoop.fs.s3a.endpoint"] = url
    return results
