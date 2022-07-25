import logging
import argparse

from kfp import components as comp
from kfp.v2 import dsl
from kfp.v2.compiler import Compiler


def generate_timestamp() -> int:
    import time
    return int(time.time())

GenerateTimeStampOp = comp.func_to_container_op(
    generate_timestamp,
    base_image="python:3.8-slim"
)

DataIngestOp = comp.load_component(
    'components/ingestion/component.yaml'
)
DataPreprocessingOp = comp.load_component(
    'components/preprocessing/component.yaml'
)

PIPELINE_ROOT = "gs://apache-beam-testing-ml-examples/pipelines"

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

CONTAINER_REGISTRY_BASE = "us-central1-docker.pkg.dev/apache-beam-testing/kfp-components-preprocessing"
PROJECT_ID = "apache-beam-testing"
PROJECT_LOCATON = "us-central1"
BUCKET_URI = "apache-beam-testing-ml-examples"
DATAFLOW_STAGING_DIR = "gs://apache-beam-testing-ml-examples/dataflow_staging"
BASE_DATA_PATH = "gs://apache-beam-testing-ml-examples/kfp-example"

@dsl.pipeline(
    pipeline_root=PIPELINE_ROOT,
    name="beam-preprocessing-kfp-example",
    description="Pipeline to show an apache beam preprocessing example in KFP"
)
def pipeline():
    timestamp_generator_task = GenerateTimeStampOp()
    ingest_data_task = DataIngestOp(
        timestamp=timestamp_generator_task.output,
        base_data_path=BASE_DATA_PATH
    )
    data_preprocessing_task = DataPreprocessingOp(
        ingested_dataset_path=ingest_data_task.outputs["ingested_dataset_path"],
        timestamp=timestamp_generator_task.output,
        base_data_path=BASE_DATA_PATH
    )



if __name__ == "__main__":
    Compiler().compile(
        pipeline_func=pipeline,
        package_path="pipeline.json"
    )
