import os
import tarfile
from datetime import datetime
from typing import Optional

import boto3
from ascii_library.orchestration.pipes import Engine, ExecutionMode
from ascii_library.orchestration.pipes.spark_script_abc import SparkScriptPipes
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


class Step_1(SparkScriptPipes):
    def execute_business_logic(  # noqa: C901
        self,
        context,
        execution_mode: ExecutionMode,
        partition_key: Optional[str],
        spark: SparkSession,
        engine: Engine,
    ):
        start_time = datetime.now().isoformat()
        bucket_name = "fast-ai-nlp"
        key = "amazon_review_full_csv.tgz"
        local_data_path = "/tmp/fast-ai-nlp"
        if not os.path.exists(local_data_path):
            os.makedirs(local_data_path)
        if engine == Engine.Local:
            s3client = boto3.client(
                "s3",
                use_ssl=False,
                aws_access_key_id=os.environ["ASCII_AWS_ACCESS_KEY_ID"],
                aws_secret_access_key=os.environ["ASCII_AWS_SECRET_ACCESS_KEY"],
            )
        else:
            s3client = boto3.client("s3", use_ssl=False)

        schema = StructType(
            [
                StructField("rating", IntegerType(), True),
                StructField("title", StringType(), True),
                StructField("review", StringType(), True),
            ]
        )

        local_tar_path = os.path.join(local_data_path, key.split("/")[-1])
        s3client.download_file(bucket_name, key, local_tar_path)
        if local_tar_path.endswith("tgz"):
            with tarfile.open(local_tar_path, "r:gz") as tar:
                tar.extractall(path=local_data_path)

        csv_file_path = os.path.join(
            local_data_path, "amazon_review_full_csv", "train.csv"
        )
        df = spark.read.csv(csv_file_path, header=False, schema=schema)
        df.printSchema()
        words_df = df.withColumn("word", explode(split(df.review, " ")))
        word_count_df = words_df.groupBy("word").count()
        word_count_df.show()
        output_bucket_name = "ascii-supply-chain-research-results"
        output_key = "word_count/amazon_review_word_count"
        word_count_df.write.mode("overwrite").parquet(
            f"s3a://{output_bucket_name}/{output_key}"
        )
        end_time = datetime.now().isoformat()

        context.report_asset_materialization(
            metadata={
                "time_start": start_time,
                "time_end": end_time,
            },
            data_version="1.0",
        )


if __name__ == "__main__":
    Step_1()
