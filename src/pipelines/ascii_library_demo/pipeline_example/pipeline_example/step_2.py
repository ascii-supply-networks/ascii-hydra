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
        input_key = "word_count/amazon_review_word_count"
        word_count_df = spark.read.parquet(f"s3a://{input_bucket_name}/{input_key}")
        avg_word_length_df = word_count_df.withColumn(
            "word_length", length(word_count_df.word)
        )
        avg_length_df = avg_word_length_df.agg(
            avg("word_length").alias("avg_word_length")
        )
        avg_length_df.show()
        num_distinct_words = word_count_df.select("word").distinct().count()
        avg_word_length = avg_length_df.collect()[0]["avg_word_length"]

        end_time = datetime.now().isoformat()

        context.report_asset_materialization(
            metadata={
                "time_start": start_time,
                "time_end": end_time,
                "num_distinct_words": num_distinct_words,
                "avg_word_length": avg_word_length,
            },
            data_version="1.0",
        )


if __name__ == "__main__":
    Step_2()
