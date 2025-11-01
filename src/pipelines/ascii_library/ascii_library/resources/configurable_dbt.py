from typing import Any, Dict

import dagster as dg
from dagster_dbt import DbtCliResource  # pyrefly: ignore


class DbtCliResourceWithConfig(dg.ConfigurableResource):
    dbt_project: DbtCliResource
    dbt_vars: Dict[str, Any]
