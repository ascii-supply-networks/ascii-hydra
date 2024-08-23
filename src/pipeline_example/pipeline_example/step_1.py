from datetime import datetime
from typing import Optional

from ascii_library.orchestration.pipes import Engine, ExecutionMode
from ascii_library.orchestration.pipes.io_paths_cc import ccc_version
from ascii_library.orchestration.pipes.spark_script_abc import SparkScriptPipes
from pyspark.sql import SparkSession


class Step_1(SparkScriptPipes):
    def execute_business_logic(  # noqa: C901
        self,
        context,
        execution_mode: ExecutionMode,
        partition_key: Optional[str],
        spark: SparkSession,
        engine: Engine,
    ):
        """
        Using commoncrawl CC-MAIN data and the parquet index
        for a list of seed node URLs extract

        - pre-process and cleanup seed node files to prevent common issues. Log out broken names
        - metadata about each seed node
        - suitable wark file names
        """

        start_time = datetime.now().isoformat() 
        df = spark.createDataFrame(
            [
                ("sue", 32),
                ("li", 3),
                ("bob", 75),
                ("heo", 13),
            ],
            ["first_name", "age"],
        )
        df.printSchema()
        end_time = datetime.now().isoformat() 

        context.report_asset_materialization(
            metadata={
                "time_start": start_time,
                "time_end": end_time,
            },
            data_version=ccc_version,
        )


if __name__ == "__main__":
    Step_1()
