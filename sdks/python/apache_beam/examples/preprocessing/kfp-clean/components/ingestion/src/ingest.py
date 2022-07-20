"""Dummy ingestion function that fetches data from one file and simply copies it to another."""

import logging
import argparse

from kfp.v2.dsl import Output, Dataset

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger("INGESTION")

def parse_args():
    """Parse ingestion arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data-source-uri", type=str,
        help="Source uri to ingest data from.")
    parser.add_argument(
        "--ingested-dataset", type=Output[Dataset],
        help="The target directory for the ingested dataset.")
    return parser.parse_args()


def dummy_ingest_data(
    data_source_uri: str,
    ingested_dataset: Output[Dataset]):
    """Dummy data ingestion step that returns an uri
    to the data it has 'ingested' as jsonlines.

    Args:
        data_ingestion_target (str): uri to the data that was scraped and 
        ingested by the component"""
    # imports used by the lightweight kfp component must
    # be imported inside the component definition

    
    with open(data_source_uri, 'r') as input_file:
        jsonlines = input_file.readlines()
    LOGGER.info("Done reading data from source uri %s", data_source_uri)
    
    with open(ingested_dataset.path, 'w') as output_file:
        output_file.writelines(jsonlines)
    LOGGER.info("Done writing data to target uri %s", ingested_dataset.path)


if __name__ == "__main__":
    args = parse_args()
    dummy_ingest_data(**vars(args))