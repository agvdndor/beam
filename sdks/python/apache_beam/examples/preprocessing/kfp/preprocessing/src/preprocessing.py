import json
import urllib
import io
import re
import requests
from typing import Dict, Union

import torch
import torchvision.transforms as T
import torchvision.transforms.functional as TF
from PIL import Image, UnidentifiedImageError
import numpy as np
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

LOGIT_LAPLACE_EPS = 0.1

PROJECT_ID = "apache-beam-testing"
BUCKET_URI = "apache-beam-testing-ml-examples"
LOCATION = "us-central1"
STAGING_DIR = "gs://apache-beam-testing-ml-examples/dataflow_staging"


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


def run_pipeline():

    beam_options = PipelineOptions(
        runner='DataflowRunner',
        project=PROJECT_ID,
        job_name='preprocessing-test',
        temp_location=STAGING_DIR,
        region=LOCATION
    )
    
    input_files = "./*.jsonl"
    with beam.Pipeline() as pipeline:
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
            | "Write to Avro files" >> beam.io.WriteToAvro(file_path_prefix="./coco_preprocessed",
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


if __name__ == "__main__":
    run_pipeline()