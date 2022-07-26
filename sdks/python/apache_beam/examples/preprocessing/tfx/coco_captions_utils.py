"""Implementation of the tfx pipeline functions for the coco captions example."""
import tempfile
import json
import sys

from pyparsing import WordStart


import tensorflow as tf
import tensorflow_transform as tft
from tfx import v1 as tfx
from tfx.components.trainer.fn_args_utils import FnArgs
import tensorflow_transform.beam as tft_beam

def run_fn(fn_args: FnArgs) -> None:
  """Build the TF model and train it."""
  model = tf.keras.applications.vgg16.VGG16(
    include_top=True,
    weights='imagenet',
    input_tensor=None,
    input_shape=None,
    pooling=None,
    classes=1000,
    classifier_activation='softmax'
    )
  # Save model to fn_args.serving_model_dir.
  model.save(fn_args.serving_model_dir)

def preprocessing_fn(inputs):
  # convert the captions to lowercase 
  # split the captions into separate words
  lower = tf.strings.lower(inputs["caption"])
  words = tf.strings.split(lower, sep=' ')
  # compute the vocabulary of the captions during a full pass
  # over the dataset and use this to tokenize.
  tokenized = tft.compute_and_apply_vocabulary(words)
  return {
     'words': words,
     'tokenized': tokenized,
  }

if __name__ == "__main__":
    # raw_data = [json.loads(jsonline) for jsonline in ["""{"image_id": 203564, "id": 37, "caption": ["A", "bicycle", "replica", "with", "a", "clock", "as", "the", "front", "wheel"], "image_url": "http://farm8.staticflickr.com/7366/9643253026_86d6e38def_z.jpg", "image_license": "Attribution License"}""",
    #                                           """{"image_id": 179765, "id": 38, "caption": ["A", "black", "Honda", "motorcycle", "parked", "in", "front", "of", "a", "garage"], "image_url": "http://farm3.staticflickr.com/2824/10213933686_6936eb402b_z.jpg", "image_license": "Attribution-NonCommercial-NoDerivs License"}""",
    #                                           """{"image_id": 322141, "id": 49, "caption": ["A", "room", "with", "blue", "walls", "and", "a", "white", "sink", "and", "door"], "image_url": "http://farm8.staticflickr.com/7026/6388965173_92664a0d78_z.jpg", "image_license": "Attribution-NonCommercial-ShareAlike License"}"""]]

    raw_data = [json.loads(jsonline) for jsonline in ["""{"image_id": 203564, "id": 37, "caption": "A bicycle replica with a clock as the front wheel.", "image_url": "http://farm8.staticflickr.com/7366/9643253026_86d6e38def_z.jpg", "image_license": "Attribution License"}""",
                                                      """{"image_id": 179765, "id": 38, "caption": "A black Honda motorcycle parked in front of a garage.", "image_url": "http://farm3.staticflickr.com/2824/10213933686_6936eb402b_z.jpg", "image_license": "Attribution-NonCommercial-NoDerivs License"}""",
                                                      """{"image_id": 322141, "id": 49, "caption": "A room with blue walls and a white sink and door.", "image_url": "http://farm8.staticflickr.com/7026/6388965173_92664a0d78_z.jpg", "image_license": "Attribution-NonCommercial-ShareAlike License"}"""]]

    # raw_data = [json.loads(jsonline) for jsonline in ["""{"caption": "this is a test string"}"""]]
    print(raw_data)
    feature_spec = dict(caption=tf.io.FixedLenFeature([], tf.string))
    raw_data_metadata = tft.DatasetMetadata.from_feature_spec(feature_spec)

    with tft_beam.Context(temp_dir=tempfile.mkdtemp()):
        transformed_dataset, transform_fn = (
            (raw_data, raw_data_metadata)
            | tft_beam.AnalyzeAndTransformDataset(preprocessing_fn))
    transformed_data, transformed_metadata = transformed_dataset
    print(transformed_data)