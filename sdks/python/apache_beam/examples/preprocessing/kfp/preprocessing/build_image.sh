# Set some variables
GCR_BASE_PATH=us-central1-docker.pkg.dev/apache-beam-testing/kfp-components-preprocessing
IMAGE_PATH="preprocessing"
IMAGE_TAG="latest"

# Create full name of the image
FULL_IMAGE_NAME=${GCR_BASE_PATH}/${IMAGE_PATH}:${IMAGE_TAG}
echo $FULL_IMAGE_NAME

gcloud builds submit . -t "$FULL_IMAGE_NAME"