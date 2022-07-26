import os
from grpc import composite_channel_credentials

from importlib_metadata import metadata

import tensorflow as tf
import tensorflow_transform as tft
import tensorflow_transform.beam as tft_beam
from tfx import v1 as tfx

def create_pipeline():
    example_gen = tfx.components.CsvExampleGen(
            input_base="./data"
        )

    # Computes statistics over data for visualization and example validation.
    statistics_gen = tfx.components.StatisticsGen(
        examples=example_gen.outputs['examples'])

    schema_gen = tfx.components.SchemaGen(
            statistics=statistics_gen.outputs['statistics'],
            infer_feature_shape=True)

    transform = tfx.components.Transform(
        examples=example_gen.outputs['examples'],
        schema=schema_gen.outputs['schema'],
        module_file="coco_captions_utils.py"
    )

    trainer = tfx.components.Trainer(
        module_file="coco_captions_utils.py",
        examples=transform.outputs['transformed_examples'],
        transform_graph=transform.outputs['transform_graph'])

    components = [
        example_gen,
        statistics_gen,
        schema_gen,
        transform,
        trainer
    ]
    
    return tfx.dsl.Pipeline(
        pipeline_name="pipeline",
        pipeline_root="./tfx",
        components=components,
        enable_cache=True,
        metadata_connection_config=tfx.orchestration.metadata
        .sqlite_metadata_connection_config("./metadata"),
        beam_pipeline_args=[
            '--direct_running_mode=multi_processing',
            # 0 means auto-detect based on on the number of CPUs available
            # during execution time.
            '--direct_num_workers=0',
        ]
    )

if __name__ == "__main__":
    tfx.orchestration.LocalDagRunner().run(
        create_pipeline()
    )

