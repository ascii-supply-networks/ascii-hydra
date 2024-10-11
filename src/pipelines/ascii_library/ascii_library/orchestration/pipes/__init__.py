from dataclasses import dataclass
from enum import Enum
from typing import Optional


class LibraryKind(Enum):
    Pypi = "pypi"
    Wheel = "whl"


@dataclass
class LibraryConfig:
    """
    - For pypi the PYPI library name and version
    - For whl: the file path to the library (absolute i.e) with s3:xxx or dbfs:/xxx
    Version should include the >=1.1.1 in case a specific version (range) or == if a specific version should be used
    """

    kind: LibraryKind
    name_id: str
    version: Optional[str] = None


class Engine(Enum):
    Local = "pyspark"
    Databricks = "databricks"
    EMR = "emr"


def get_engine_by_value(value: str) -> Engine:
    for engine in Engine:
        if engine.value == value:
            return engine
    raise ValueError(f"No matching Engine for value: {value}")


class ExecutionMode(Enum):
    Full = "full"
    SmallDevSampleS3 = "small_dev_sample_s3"
    # local mode MUST be paired with Engine.Local (pyspark local)
    SmallDevSampleLocal = "small_dev_sample_local"
