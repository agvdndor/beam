from email import parser
import logging
import argparse

import kfp
from kfp import components as comp
from kfp.v2 import dsl
from kfp.v2.dsl import (Artifact, Dataset, Input, Output)
from kfp.v2.compiler import Compiler

from ingestion.ingest import dummy_ingest_data


PIPELINE_ROOT = "gs://apache-beam-testing-ml-examples/pipelines"

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

preprocess_data_op = comp.load_component(
    'preprocessing/component.yaml')

def parse_args():
    """Parse KFP arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data-ingestion-target-uri", type=str,
        help="The output directory to which the ingested data is written as jsonlines.")
    parser.add_argument(
        "--preprocessed-dataset-output-uri", type=str,
        help="The output directory for the preprocessed serialized dataset train and test splits.")
    parser.add_argument(
        "--train-test-split", type=float, default=0.8,
        help="Fractional part of the train dataset compared to the entire dataset.")
    return parser.parse_args()


@dsl.pipeline(
    pipeline_root=PIPELINE_ROOT,
    name="beam-preprocessing-kfp-example",
    description="Pipeline to show an apache beam preprocessing example in KFP"
)
def full_pipeline(
    data_ingestion_target_uri: str,
    preprocessed_dataset_output_uri: str,
    train_test_split: float
):
    ingest_data_task = dummy_ingest_data(
        data_ingestion_src_uri=data_ingestion_target_uri
    )

    preprocess_data_task = preprocess_data_op(
        ingested_dataset=ingest_data_task.outputs['ingested_dataset'],
        train_test_split=train_test_split
    )


if __name__ == "__main__":
    args = parse_args()

    Compiler().compile(
        pipeline_func=full_pipeline,
        package_path="pipeline.json"
    )
