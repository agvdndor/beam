"""Lightweight dummy component for KFP data ingestion."""
import logging
from typing import NamedTuple

import kfp
from kfp.v2.dsl import component, Dataset, Output, OutputPath, InputPath


@component(
    base_image="python:3.8"
)
def dummy_ingest_data(
    data_ingestion_src_uri: str,
    data_ingestion_target: Output[Dataset]
) -> NamedTuple:
    """Dummy data ingestion step that returns an uri
    to the data it has 'ingested' as jsonlines
    https://www.kubeflow.org/docs/components/pipelines/sdk-v2/v2-component-io/.

    Args:
        data_ingestion_target (str): uri to the data that was scraped and 
        ingested by the component"""
    # imports used by the lightweight kfp component must
    # be imported inside the component definition
    import json
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("INGESTION")
    
    with open(data_ingestion_src_uri, 'r') as input_file:
        jsonlines = input_file.readlines()
    logger.info("Done reading data from source uri %s", data_ingestion_src_uri)
    
    with open(data_ingestion_target.path, 'w') as output_file:
        output_file.writelines("\n".join(jsonlines))
    logger.info("Done writing data to target uri %s", data_ingestion_target.path)



