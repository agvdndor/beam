"""Dummy ingestion function that fetches data from one file and simply copies it to another."""

import logging
import re
import json
import io
import argparse
import time

import requests
import torch
import torchvision.transforms as T
import torchvision.transforms.functional as TF
from PIL import Image, UnidentifiedImageError
import numpy as np
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from kfp.v2.dsl import component, Dataset, Output, OutputPath, InputPath, Input

LOGIT_LAPLACE_EPS = 0.1
PROJECT_ID = "apache-beam-testing"
BUCKET_URI = "apache-beam-testing-ml-examples"
LOCATION = "us-central1"
STAGING_DIR = "gs://apache-beam-testing-ml-examples/dataflow_staging"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PREPROCESSING")


def parse_args():
    """Parse preprocessing arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ingested-dataset", type=Input[Dataset],
        help="Source uri to ingest data from.")
    parser.add_argument(
        "--preprocessed-dataset", type=Output[Dataset],
        help="The target directory for the ingested dataset.")
    return parser.parse_args()


def preprocess_dataset(
    ingested_dataset: Input[Dataset],
    preprocessed_dataset: Output[Dataset]):
    """Dummy data ingestion step that returns an uri
    to the data it has 'ingested' as jsonlines.

    Args:
        data_ingestion_target (str): uri to the data that was scraped and 
        ingested by the component"""

    logger.info("ingested-dataset-uri %s", ingested_dataset.uri)
    logger.info("preprocessed-dataset-uri %s", preprocessed_dataset.uri)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(
        runner='DataflowRunner',
        project=PROJECT_ID,
        job_name=f'preprocessing-test-{int(time.time())}',
        temp_location=STAGING_DIR,
        region=LOCATION,
        requirements_file="/requirements.txt",
        save_main_session = True,
        experiments = ["use_runner_v2"],
    )

    input_files = ingested_dataset.uri
    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | "Read input jsonl file" >> beam.io.ReadFromText(input_files)
            | "Load json" >> beam.Map(json.loads)
            | "Filter licenses" >> beam.Filter(valid_license)
            | "Download image from URL" >> beam.ParDo(DownloadImageFromURL())
            | "Filter on valid images" >> beam.Filter(lambda el: el['image'] is not None)
            | "Resize image" >> beam.ParDo(ResizeImage(size=(256,256)))
            | "Rescale the image pixel distribution" >> beam.ParDo(RescaleImagePixelValues())
            | "Clean Text" >> beam.ParDo(CleanText())
            | "Serialize Example" >> beam.ParDo(SerializeExample())
            | "Write to Avro files" >> beam.io.WriteToAvro(file_path_prefix=preprocessed_dataset.uri,
                                                           schema={"namespace": "preprocessing.example",
                                                                   "type": "record",
                                                                   "name": "Sample",
                                                                   "fields": [
                                                                           {"name": "id", "type": "int"},
                                                                           {"name": "caption", "type": "string"},
                                                                           {"name": "image", "type": "bytes"}
                                                                       ]},
                                                           file_name_suffix=".avro")
            # | "print" >> beam.Map(print)
        )


class DownloadImageFromURL(beam.DoFn):
    """DoFn to download the images from their uri."""
    def process(self, element):
        response = requests.get(element['image_url'])
        try:
            image = Image.open(io.BytesIO(response.content))
            image = T.ToTensor()(image)
            element['image'] = image
        except UnidentifiedImageError:
            element['image'] = None
        return [element]


class ResizeImage(beam.DoFn):
    "DoFn to resize the elememt's PIL image to the target resolution."
    def process(self, element, size=(256,256)):
        element['image'] = TF.resize(element['image'], size)
        return [element]


class CleanText(beam.DoFn):
    def process(self, element):
        text = element['caption']

        text = text.lower()  # lower case
        text = re.sub(r"http\S+", "", text)  # remove urls
        text = re.sub("\s+", " ", text)  # remove extra spaces (including \n and \t)
        text = re.sub("[()[\].,|:;?!=+~\-\/{}]", ",", text)  # all puncutation are replace w commas
        text = f" {text}"  # always start with a space
        text = text.strip(',')  #  remove commas at the start or end of the caption
        text = text[:-1] if text and text[-1] == "," else text
        text = text[1:] if text and text[0] == "," else text

        element["preprocessed_caption"] = text
        return [element]


class RescaleImagePixelValues(beam.DoFn):
    """DoFn to tranform the distribution of the pixel values necessary for the
    Image AutoEncoder."""
    def process(self, element):
        image = element['image']
        element['image'] = (1 - 2 * LOGIT_LAPLACE_EPS) * image + LOGIT_LAPLACE_EPS
        return [element]


def valid_license(element):
    license = element['image_license']
    return license in ["Attribution License", "No known copyright restrictions"]


class SerializeExample(beam.DoFn):
    def process(self, element):
        buffer = io.BytesIO()
        torch.save(element['image'], buffer)
        buffer.seek(0)
        element['image'] = buffer.read()
        return [element]


if __name__ == "__main__":
    args = parse_args()
    preprocess_dataset(**vars(args))
