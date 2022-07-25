"""Submit the compiled job to vertex ai pipelines."""
import time
import argparse

import google.cloud.aiplatform as aip


PROJECT_ID = "apache-beam-testing"
BUCKET_URI = "apache-beam-testing-ml-examples"
LOCATION = "us-central1"
PIPELINE_ROOT = "gs://apache-beam-testing-ml-examples/pipelines"


# def parse_args():
#     """Parse KFP arguments"""
#     parser = argparse.ArgumentParser()
#     parser.add_argument(
#         "--data-source-uri", type=str,
#         help="The uri to ingest the data from.")
#     parser.add_argument(
#         "--train-test-split", type=float, default=0.8,
#         help="Fractional part of the train dataset compared to the entire dataset.")
#     return parser.parse_args()


if __name__ == "__main__":
    aip.init(project=PROJECT_ID, staging_bucket=BUCKET_URI, location=LOCATION)

    # args = parse_args()

    job = aip.PipelineJob(
        display_name=f"beam-preprocessing-{time.time()}",
        template_path="./pipeline.json",
        # parameter_values=vars(args),
        pipeline_root=PIPELINE_ROOT,
        # enable_caching=False
    )

    job.run()
