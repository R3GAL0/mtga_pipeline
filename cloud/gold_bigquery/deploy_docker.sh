#!/bin/bash
set -euo pipefail
IMAGE="mtga-gold"

# build the image
# docker build -t gcr.io/mtgapipeline/$IMAGE:latest .
docker build --no-cache -t gcr.io/mtgapipeline/$IMAGE:latest .

# tag and push the image to gcp
docker tag gcr.io/mtgapipeline/$IMAGE:latest northamerica-northeast2-docker.pkg.dev/mtgapipeline/cloud-run-source-deploy/$IMAGE:latest
docker push northamerica-northeast2-docker.pkg.dev/mtgapipeline/cloud-run-source-deploy/$IMAGE:latest

# Update the GCP cloud run job and execute
gcloud run jobs create $IMAGE-job \
--image northamerica-northeast2-docker.pkg.dev/mtgapipeline/cloud-run-source-deploy/$IMAGE:latest \
--region northamerica-northeast2
gcloud run jobs execute $IMAGE-job --region northamerica-northeast2
