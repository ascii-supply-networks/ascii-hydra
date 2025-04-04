import random
import string
from datetime import datetime
from typing import Optional

from ascii_library.orchestration.pipes import Engine, ExecutionMode
from ascii_library.orchestration.pipes.spark_script_abc import SparkScriptPipes
from pyspark.sql import SparkSession
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
        def generate_random_string(length=10):
            """Generate a random alphanumeric string of given length."""
            return "".join(
                random.choices(string.ascii_letters + string.digits, k=length)
            )

        start_time = datetime.now().isoformat()
        num_rows = 100
        data = [
            (i, generate_random_string(15), random.randint(1, 100))
            for i in range(num_rows)
        ]
        schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("random_text", StringType(), False),
                StructField("value", IntegerType(), False),
            ]
        )
        df = spark.createDataFrame(data, schema)
        df.show(10)
        output_bucket_name = "ascii-supply-chain-research-results"
        output_key = "random_data/random_data_parquet"
        output_path = f"s3a://{output_bucket_name}/{output_key}"
        df.write.mode("overwrite").parquet(output_path)
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
