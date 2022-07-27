from kedro.pipeline import node, Pipeline
from .nodes import quality_sample

def data_quality(**kwargs):
    return Pipeline(
        [
            node(
                func=quality_sample,
                inputs="prm_sample",
                outputs="qua_sample",
                name="quality_sample",
            )
        ]
    )