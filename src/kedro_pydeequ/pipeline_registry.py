"""Project pipelines."""
from typing import Dict

from kedro.pipeline import Pipeline, pipeline
from .data_engineering.pipeline import data_engineering
from .data_quality.pipeline import data_quality

def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.
    """

    return {"__default__": pipeline([data_engineering(),data_quality()])}
