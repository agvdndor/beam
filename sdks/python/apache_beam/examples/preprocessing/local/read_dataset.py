import json
import urllib
import io
import re
import requests
from typing import Dict, Union

import torch
import torchvision.transforms as T
import torchvision
import torchvision.transforms.functional as TF

from PIL import Image, UnidentifiedImageError
import numpy as np
import apache_beam as beam
from apache_beam.io.avroio import WriteToAvro, ReadFromAvro


class DeserializeExample(beam.DoFn):
    def process(self, element):
        element['image'] = torch.load(io.BytesIO(element['image']))
        with open("test.jpg", 'wb') as file_:
            torchvision.utils.save_image(element['image'], file_)
        return [element]


def run_pipeline():
    input_files = "./*.avro"
    with beam.Pipeline() as pipeline:
        (
            pipeline
            | "Read input avro file" >> beam.io.ReadFromAvro(input_files)
            | "Deserialize" >> beam.ParDo(DeserializeExample())
            | "Print" >> beam.Map(print)
        )

if __name__ == "__main__":
    run_pipeline()