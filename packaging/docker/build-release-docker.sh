#!/bin/bash

set -euxo pipefail

DOCKER_ROOT=$(realpath $(dirname ${BASH_SOURCE[0]}))
BUILD_OUTPUT=$(realpath ${DOCKER_ROOT}/../..)

echo Docker root:  $DOCKER_ROOT
echo Build output: $BUILD_OUTPUT

cd ${DOCKER_ROOT}

## eg: CMAKE_PROJECT_VERSION:STATIC=7.0.0
FDB_VERSION=$(grep CMAKE_PROJECT_VERSION\: ${BUILD_OUTPUT}/CMakeCache.txt | cut -d '=' -f 2)

# Options (passed via environment variables)

# Feel free to customize the image tag.
# TODO: add a mechanism to set TAG=FDB_VERSION when we're building public releases.
TAG=${TAG:-${FDB_VERSION}-${OKTETO_NAME}}
ECR=${ECR:-112664522426.dkr.ecr.us-west-2.amazonaws.com}

echo Building with tag ${TAG}

# Login to ECR
# TODO: Move this to a common place instead of repeatedly copy-pasting it.
aws ecr get-login-password | docker login --username AWS --password-stdin ${ECR}

docker pull ${ECR}/ubuntu:18.04
docker tag ${ECR}/ubuntu:18.04 ubuntu:18.04
docker pull ${ECR}/python:3.9-slim
docker tag ${ECR}/python:3.9-slim python:3.9-slim

# derived variables
IMAGE=foundationdb/foundationdb:${TAG}
SIDECAR=foundationdb/foundationdb-kubernetes-sidecar:${TAG}-1
STRIPPED=${STRIPPED:-false}

WEBSITE_BIN_DIR=website/downloads/${FDB_VERSION}/linux/
TARBALL=${WEBSITE_BIN_DIR}/fdb_${FDB_VERSION}.tar.gz
mkdir -p ${WEBSITE_BIN_DIR}

if $STRIPPED; then
  tar -C ~/build_output/packages/ -zcvf ${TARBALL} bin lib
  cp ~/build_output/packages/lib/libfdb_c.so ${WEBSITE_BIN_DIR}/libfdb_c_${FDB_VERSION}.so
else
  tar -C ~/build_output/ -zcvf ${TARBALL} bin lib
  cp ~/build_output/lib/libfdb_c.so ${WEBSITE_BIN_DIR}/libfdb_c_${FDB_VERSION}.so
fi

BUILD_ARGS="--build-arg FDB_WEBSITE=file:///mnt/website "
BUILD_ARGS+="--build-arg FDB_VERSION=$FDB_VERSION "
BUILD_ARGS+="--build-arg FDB_ADDITIONAL_VERSIONS=$FDB_VERSION"

docker build -t ${IMAGE} ${BUILD_ARGS} -f release/Dockerfile .
docker build -t ${SIDECAR} ${BUILD_ARGS} -f sidecar/Dockerfile .

docker tag ${IMAGE} ${ECR}/${IMAGE}
docker tag ${SIDECAR} ${ECR}/${SIDECAR}

docker push ${ECR}/${IMAGE}
docker push ${ECR}/${SIDECAR}
