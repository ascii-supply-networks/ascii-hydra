from dataclasses import dataclass
from enum import Enum
from typing import Optional


class LibraryKind(Enum):
    Pypi = "pypi"
    Wheel = "whl"


@dataclass
class LibraryConfig:
    """Configuration for a Python library dependency.

    - For pypi: The PyPI library name and version.
    - For whl: The file path to the library (e.g., s3://... or dbfs:/...).

    Version specifiers should be included (e.g., `>=1.1.1` for a range or `==2.0.0` for a specific version).
    """

    kind: LibraryKind
    name_id: str
    version: Optional[str] = None
    extra_flags: Optional[str] = None


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
