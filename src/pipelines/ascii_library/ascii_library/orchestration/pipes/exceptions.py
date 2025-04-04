from dagster import DagsterError


class CustomPipesException(DagsterError):
    """Custom exception for handling errors with cloud job flow."""

    def __init__(self, message):
        super().__init__(message)
