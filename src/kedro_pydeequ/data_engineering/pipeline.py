from kedro.pipeline import node, Pipeline
from .nodes import primary_sample

def data_engineering(**kwargs):
    return Pipeline(
        [
            node(
                func=primary_sample,
                inputs="sample",
                outputs="prm_sample",
                name="preprocessing_sample",
            )
        ]
    )