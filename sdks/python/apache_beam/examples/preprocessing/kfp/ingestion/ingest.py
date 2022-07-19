"""Lightweight dummy component for KFP data ingestion."""
import logging
from typing import NamedTuple

import kfp
from kfp.v2.dsl import component, Dataset, Output


@component(
    base_image="python:3.8"
)
def dummy_ingest_data(
    data_ingestion_target_uri: str
) -> NamedTuple(
    "Outputs",
    [
        ("data_uri", Output[Dataset])
    ]
):
    """Dummy data ingestion step that returns an uri
    to the data it has 'ingested' as jsonlines.

    Args:
        data_ingestion_target_uri (str): uri to the data that was scraped and 
        ingested by the component"""
    # imports used by the lightweight kfp component must
    # be imported inside the component definition
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("INGESTION")
    
    # TODO replace with actual data ingestion
    logger.info("Done ingesting data to %s", data_ingestion_target_uri)

    return data_ingestion_target_uri



