from datetime import datetime
from typing import Optional

from ascii_library.orchestration.pipes import Engine, ExecutionMode
from ascii_library.orchestration.pipes.spark_script_abc import SparkScriptPipes
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, length


class Step_2(SparkScriptPipes):
    def execute_business_logic(  # noqa: C901
        self,
        context,
        execution_mode: ExecutionMode,
        partition_key: Optional[str],
        spark: SparkSession,
        engine: Engine,
    ):
        start_time = datetime.now().isoformat()

        input_bucket_name = "ascii-supply-chain-research-results"
        input_key = "random_data/random_data_parquet"
        df = spark.read.parquet(f"s3a://{input_bucket_name}/{input_key}")
        df.show(10)
        df_with_length = df.withColumn("text_length", length(df.random_text))
        avg_length_df = df_with_length.agg(avg("text_length").alias("avg_text_length"))
        avg_length_df.show()
        num_distinct_texts = df.select("random_text").distinct().count()
        avg_value = df.agg(avg("value").alias("avg_value")).collect()[0]["avg_value"]
        avg_text_length = avg_length_df.collect()[0]["avg_text_length"]

        end_time = datetime.now().isoformat()

        context.report_asset_materialization(
            metadata={
                "time_start": start_time,
                "time_end": end_time,
                "num_distinct_texts": num_distinct_texts,
                "avg_text_length": avg_text_length,
                "avg_value": avg_value,
            },
            data_version="1.0",
        )


if __name__ == "__main__":
    Step_2()
