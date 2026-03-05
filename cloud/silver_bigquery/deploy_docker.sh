#!/bin/bash
set -euo pipefail

# build the image
docker build -t gcr.io/mtgapipeline/mtga-pipeline:latest .
# docker build --no-cache -t gcr.io/mtgapipeline/mtga-pipeline:latest .

# tag and push the image to gcp
docker tag gcr.io/mtgapipeline/mtga-pipeline:latest northamerica-northeast2-docker.pkg.dev/mtgapipeline/cloud-run-source-deploy/mtga-pipeline:latest
docker push northamerica-northeast2-docker.pkg.dev/mtgapipeline/cloud-run-source-deploy/mtga-pipeline:latest

# Update the GCP cloud run job and execute
gcloud run jobs update mtga-pipeline-job \
--image northamerica-northeast2-docker.pkg.dev/mtgapipeline/cloud-run-source-deploy/mtga-pipeline:latest \
--region northamerica-northeast2
gcloud run jobs execute mtga-pipeline-job --region northamerica-northeast2
