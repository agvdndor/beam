"""Dummy training function that loads a pretrained model from the torch hub and saves it."""

import argparse
from pathlib import Path
import time

import torch


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
  """Dummy to load a model from the torch hub and save it.

  Args:
    preprocessed_dataset_path (str): Path to the preprocessed dataset
    trained_model_path (str): Output path for the trained model
    base_artifact_path (str): path to the base directory of where artifacts can be stored for
      this component
  """
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
