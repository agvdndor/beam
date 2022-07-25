"""Dummy ingestion function that fetches data from one file and simply copies it to another."""

import logging
import argparse
from pathlib import Path


logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger("INGESTION")

def parse_args():
    """Parse ingestion arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ingested-dataset-path", type=str,
        help="Source uri to ingest data from.")
    parser.add_argument(
        "--timestamp", type=str,
        help="timestamp as identifier for the pipeline.")
    parser.add_argument(
        "--base-data-path", type=str,
        help="Source uri to ingest data from.")
    return parser.parse_args()


def dummy_ingest_data(
    ingested_dataset_path: str,
    timestamp: int,
    base_data_path: str):
    """Dummy data ingestion step that returns an uri
    to the data it has 'ingested' as jsonlines.

    Args:
        data_ingestion_target (str): uri to the data that was scraped and 
        ingested by the component"""
    # create directory to store the actual data
    target_path = f"{base_data_path}/ingestion/ingested_dataset_{timestamp}.jsonl"
    target_path_gcsfuse = target_path.replace("gs://", "/gcs/")
    Path(target_path_gcsfuse).parent.mkdir(parents=True, exist_ok=True)
    with open(target_path_gcsfuse, 'w') as f:
        f.writelines([
            """{"image_id": 318556, "id": 255, "caption": "An angled view of a beautifully decorated bathroom.", "image_url": "http://farm4.staticflickr.com/3133/3378902101_3c9fa16b84_z.jpg", "image_name": "COCO_train2014_000000318556.jpg", "image_license": "Attribution-NonCommercial-ShareAlike License"}\n""",
            """{"image_id": 476220, "id": 314, "caption": "An empty kitchen with white and black appliances.", "image_url": "http://farm7.staticflickr.com/6173/6207941582_b69380c020_z.jpg", "image_name": "COCO_train2014_000000476220.jpg", "image_license": "Attribution-NonCommercial License"}\n""",
            """{"image_id": 134754, "id": 425, "caption": "Two people carrying surf boards on a beach.", "image_url": "http://farm9.staticflickr.com/8500/8398513396_b6a1f11a4b_z.jpg", "image_name": "COCO_train2014_000000134754.jpg", "image_license": "Attribution-NonCommercial-NoDerivs License"}"""
        ])

    # the directory where the output file is created may or may not exists
    # so we have to create it.
    Path(ingested_dataset_path).parent.mkdir(parents=True, exist_ok=True)
    with open(ingested_dataset_path, 'w') as f:
        f.write(target_path)


if __name__ == "__main__":
    args = parse_args()
    dummy_ingest_data(**vars(args))