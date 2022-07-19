"""Run a compiled pipeline"""
import time

import google.cloud.aiplatform as aip

from pipeline import parse_args

PROJECT_ID = "apache-beam-testing"
BUCKET_URI = "apache-beam-testing-ml-examples"
LOCATION = "us-central1"
PIPELINE_ROOT = "gs://apache-beam-testing-ml-examples/pipelines"


if __name__ == "__main__":
    aip.init(project=PROJECT_ID, staging_bucket=BUCKET_URI, location=LOCATION)

    args = parse_args()
    print(args)

    job = aip.PipelineJob(
        display_name=f"beam-preprocessing-{time.time()}",
        template_path="./pipeline.json",
        parameter_values=vars(args),
        pipeline_root=PIPELINE_ROOT
    )

    job.run()