#!/bin/sh
set -eo pipefail

CURRENT_DIRECTORY="$( dirname -- "$0"; )"
source ${CURRENT_DIRECTORY}/shell-utils.sh
source ${CURRENT_DIRECTORY}/image-utils.sh

execution_log="$(pwd)/image-build.log"
dockerfile_fp="$(pwd)/Dockerfile"

main() {
  console "Node name: ${NODE_NAME}"

  # Example: x.x.x.x:5000
  registry_endpoint=$(resolve_registry_endpoint)
  console "Registry endpoint: ${registry_endpoint}"

  # Example: /somewhere/registry.crt
  registry_certificate_fp=$(resolve_registry_crt)
  console "Registry certificate file path: ${registry_certificate_fp}"

  # BASE_IMAGE should always be python3.8 starting from rel-2.2
  # In rel-2.6, we added support in using python3.11 as a base image.
  # TODO: In rel-2.7, we should remove the support for python3.8 and only use python3.11.
  # Example: python:3.11
  console "Base image: ${BASE_IMAGE}"

  # Example: x-y-z-some-hash
  image_tag="$(resolve_base_image_tag)-${DESTINATION_IMAGE_TAG_SUFFIX}"

  # Example: private-reg-org:5000/custom-worker:x-y-z-some-hash
  destination_image="${registry_endpoint}/${DESTINATION_IMAGE_NAME}:${image_tag}"

  console "Destination image: ${destination_image}"

  print_df
  console "Going to kick off the custom image build process."
  print_dockerfile ${dockerfile_fp}

  /kaniko/executor \
    --context=dir://$(pwd) \
    --dockerfile=${dockerfile_fp} \
    --destination=${destination_image} \
    --registry-certificate ${registry_endpoint}=${registry_certificate_fp} \
    --push-retry=5 \
    --image-fs-extract-retry=3 \
    --single-snapshot \
    --log-timestamp=true \
    --log-format=text \
    --verbosity=${KANIKO_LOG_LEVEL} \
    --ignore-path=/product_uuid

  print_df
  console "All done!"
}

main 1> >(tee -a "${execution_log}") 2>&1
