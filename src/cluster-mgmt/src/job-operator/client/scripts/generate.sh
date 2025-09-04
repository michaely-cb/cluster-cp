#!/bin/bash
set -xe

cd "$(dirname "$0")"
GEN_DIR=$(pwd -P)
REPO_DIR="$(realpath $GEN_DIR/../..)"

PROJECT_MODULE="cerebras.com/job-operator"
IMAGE_NAME="kubernetes-codegen:latest"

echo "Building codegen Docker image..."
docker build -f "${GEN_DIR}/Dockerfile" \
             -t "${IMAGE_NAME}" \
             "${REPO_DIR}"

function generate_client() {
    local CUSTOM_RESOURCE_NAME="$1"
    local CUSTOM_RESOURCE_VERSION="v1"

    cmd="./generate-groups.sh client,informer,lister \
        "$PROJECT_MODULE/client-$CUSTOM_RESOURCE_NAME" \
        "$PROJECT_MODULE/apis" \
        $CUSTOM_RESOURCE_NAME:$CUSTOM_RESOURCE_VERSION \
        --go-header-file /go/src/$PROJECT_MODULE/hack/boilerplate.go.txt \
        -v 10"
    echo "Generating $CUSTOM_RESOURCE_NAME client code..."
    docker rm clientgen || true
    docker run --name clientgen \
            -v "${REPO_DIR}:/go/src/${PROJECT_MODULE}" \
            "${IMAGE_NAME}" $cmd
    docker cp clientgen:/go/src/$PROJECT_MODULE/client-$CUSTOM_RESOURCE_NAME $REPO_DIR
    docker rm clientgen
}

generate_client namespace
generate_client system
generate_client resourcelock
