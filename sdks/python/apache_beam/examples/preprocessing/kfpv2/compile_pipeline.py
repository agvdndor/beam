from email import parser
import logging
import argparse

from kfp import components as comp
from kfp.v2 import dsl
from kfp.v2.compiler import Compiler


DataIngestOp = comp.load_component(
    'components/ingestion/component.yaml'
)
DataPreprocessingOp = comp.load_component(
    'components/preprocessing/component.yaml'
)

PIPELINE_ROOT = "gs://apache-beam-testing-ml-examples/pipelines"

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


@dsl.pipeline(
    pipeline_root=PIPELINE_ROOT,
    name="beam-preprocessing-kfp-example",
    description="Pipeline to show an apache beam preprocessing example in KFP"
)
def pipeline(
    data_source_uri: str,
    train_test_split: float
):
    ingest_data_task = DataIngestOp(
        data_source_uri=data_source_uri
    )

    preprocess_data_task = DataPreprocessingOp(
        ingested_dataset=ingest_data_task.outputs['ingested_dataset'],
    )


if __name__ == "__main__":

    Compiler().compile(
        pipeline_func=pipeline,
        package_path="pipeline.json"
    )
