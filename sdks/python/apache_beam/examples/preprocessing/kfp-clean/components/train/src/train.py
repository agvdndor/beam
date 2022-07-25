"""Dummy ingestion function that fetches data from one file and simply copies it to another."""

import logging
import argparse
from pathlib import Path
import time

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
        "--trained-model-path", type=str,
        help="Output path to the trained model.")
    parser.add_argument(
        "--base-artifact-path", type=str,
        help="Base path to store pipeline artifacts.")
    return parser.parse_args()


def train_model(
    preprocessed_dataset_path: str,
    trained_model_path: str,
    base_artifact_path: str):
    # timestamp for the component execution
    timestamp = time.time()

    # create model or load a pretrained one
    model = torch.hub.load('pytorch/vision:v0.10.0', 'vgg16', pretrained=True)

    # TODO: train on preprocessed dataset
    # <insert training loop>

    # create directory to export the model to
    target_path = f"{base_artifact_path}/training/trained_model_{timestamp}.pt"
    target_path_gcsfuse = target_path.replace("gs://", "/gcs/")
    Path(target_path_gcsfuse).parent.mkdir(parents=True, exist_ok=True)

    # save and export the model
    torch.save(model.state_dict(), target_path_gcsfuse)

    # Write the model path to the component output file
    Path(trained_model_path).parent.mkdir(parents=True, exist_ok=True)
    with open(trained_model_path, 'w') as f:
        f.write(target_path)


if __name__ == "__main__":
    args = parse_args()
    train_model(**vars(args))