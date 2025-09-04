#!/bin/sh
set -eo pipefail

CURRENT_DIRECTORY="$( dirname -- "$0"; )"
source ${CURRENT_DIRECTORY}/shell-utils.sh
source ${CURRENT_DIRECTORY}/image-utils.sh

execution_log="$(pwd)/dockerfile-generation.log"
dockerfile_fp="$(pwd)/Dockerfile"

main() {
  # Example: x.x.x.x:5000
  registry_endpoint=$(resolve_registry_endpoint)
  console "Registry endpoint: ${registry_endpoint}"

  # BASE_IMAGE should always be python3.8 starting from rel-2.2
  # In rel-2.6, we added support in using python3.11 as a base image.
  # TODO: In rel-2.7, we should remove the support for python3.8 and only use python3.11.
  # Example: python:3.11
  console "Base image: ${BASE_IMAGE}"

  # Example: private-reg-org:5000/python:3.11
  # Note that with the private registry recently (Jan 2023) being changed to use host networking,
  # we must specify the port in the endpoint for Kaniko to resolve the address properly.
  # When we pull the built images from the private registry again, the image reference should NOT
  # contain the registry port, as containerd understands how to resolve the endpoint with /etc/containerd/certs.d.
  # TL;DR the specifying port is a bespoke configuration required by the Kaniko project.
  dockerfile_base_image="${registry_endpoint}/$(resolve_base_image_name_tag)"
  console "Dockerfile base image: ${dockerfile_base_image}"

  console "Going to generate and persist Dockerfile in ${dockerfile_fp}."

  # Directly pip installing in the base image do not trigger an image snapshot properly
  # This limitation needs to be worked around by copying a dummy file from another image into the base image to trigger the snapshot.
  echo "FROM ${registry_endpoint}/alpine-containerd:${ALPINE_CONTAINERD_TAG} as snapshot-helper" > ${dockerfile_fp}
  echo "RUN touch /tmp/snapshot-trigger" >> ${dockerfile_fp}

  echo "FROM ${dockerfile_base_image}" >> ${dockerfile_fp}
  echo "COPY --from=snapshot-helper /tmp/snapshot-trigger /tmp/snapshot-trigger" >> ${dockerfile_fp}
  INSTALL_CMD="RUN pip install --no-deps --no-cache-dir ${PIP_OPTIONS} "
  if [ $(echo ${DEPENDENCY_LIST} | jq -r 'length') -gt 0 ]; then
    for dep in $(echo ${DEPENDENCY_LIST} | jq -r '.[]'); do
      INSTALL_CMD="${INSTALL_CMD} ${dep}"
    done
    echo "${INSTALL_CMD}" >> ${dockerfile_fp}
  fi

  print_dockerfile ${dockerfile_fp}
  console "Dockerfile generation completed!"
}

main 1> >(tee -a "${execution_log}") 2>&1
