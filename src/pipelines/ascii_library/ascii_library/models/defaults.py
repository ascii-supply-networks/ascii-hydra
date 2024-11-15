import os
from pathlib import Path

from loguru import logger


def set_cache_dir(cache_dir_base="/data/raid5/data/ascii/models/"):
    """Check if on the server and root path exists, if this is the case set the models base directory.

    > WARNING: This function MUST be executed before importing `huggingface, transformers`, ... or any other model library
    """
    data_path = Path(cache_dir_base)

    if not data_path.exists():
        logger.info("Not on server - noop not changing caching directory")
        # data_path.mkdir(parents=True, exist_ok=True)
    else:
        hf_home = data_path / "hf"
        hf_home.mkdir(parents=True, exist_ok=True)
        os.environ["HF_HOME"] = str(data_path)

        hf_datasets = hf_home / "datasets"
        hf_datasets.mkdir(parents=True, exist_ok=True)
        os.environ["HF_DATASETS_CACHE"] = str(hf_datasets)

        hf_transformers = hf_home / "models"
        hf_transformers.mkdir(parents=True, exist_ok=True)
        os.environ["TRANSFORMERS_CACHE"] = str(hf_transformers)
