"""Dummy ingestion function that fetches data from one file and simply copies it to another."""

import logging
import argparse
from pathlib import Path

import torch

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger("INGESTION")

def parse_args():
    """Parse ingestion arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--preprocessed-dataset-path", type=str,
        help="Path to the preprocessed dataset.")
    parser.add_argument(
        "--timestamp", type=str,
        help="Timestamp as identifier for the pipeline.")
    parser.add_argument(
        "--base-artifact-path", type=str,
        help="Base path to store pipeline artifacts.")
    return parser.parse_args()


def train_model(
    preprocessed_dataset_path: str,
    timestamp: int,
    base_artifact_path: str):

    torch.hub.load(model = torch.hub.load('pytorch/vision:v0.10.0', 'vgg16', pretrained=True))


if __name__ == "__main__":
    args = parse_args()
    train_model(**vars(args))