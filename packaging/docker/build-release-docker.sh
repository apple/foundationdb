#!/bin/bash
set -euxo pipefail

# This is designed to be run inside an environment with foundationdb checked out at ~/src/foundationdb.
# The foundationdb build will write its output to ~/build_output
FDB_SRC=${HOME}/src/foundationdb
FDB_BUILD=${HOME}/build_output

FDB_VERSION=$(grep '  VERSION ' ${FDB_SRC}/CMakeLists.txt | tr -s ' ' ' ' | cut -d ' ' -f 3)

# Options (passed via environment variables)

# Feel free to customize the image tag.
# TODO: add a mechanism to set TAG=FDB_VERSION when we're building public releases.
TAG=${TAG:-${FDB_VERSION}-${OKTETO_NAME}}
ECR=${ECR:-112664522426.dkr.ecr.us-west-2.amazonaws.com}

echo Building with tag ${TAG}

# TODO: This is a copy of the commonly-used 'cmk' function.
cmake -S ${FDB_SRC} -B ${FDB_BUILD} \
   -D USE_CCACHE=ON -D USE_WERROR=ON -D RocksDB_ROOT=/opt/rocksdb-6.10.1 -D RUN_JUNIT_TESTS=ON -D RUN_JAVA_INTEGRATION_TESTS=ON \
   -G Ninja

ninja -C ${FDB_BUILD} -j 84

# derived variables
IMAGE=foundationdb/foundationdb:${TAG}
SIDECAR_IMAGE=foundationdb/foundationdb-kubernetes-sidecar:${TAG}-1

cd ${FDB_BUILD}/packages/docker

WEBSITE_BIN_DIR=website/downloads/${FDB_VERSION}/linux/
TARBALL=${WEBSITE_BIN_DIR}/fdb_${FDB_VERSION}.tar.gz

mkdir -p ${WEBSITE_BIN_DIR}
tar -C ~/build_output/packages/ -zcvf ${TARBALL} bin lib
cp ~/build_output/packages/lib/libfdb_c.so ${WEBSITE_BIN_DIR}/libfdb_c_${FDB_VERSION}.so

# Login to ECR
# TODO: Move this to a common place instead of repeatedly copy-pasting it.
aws ecr get-login-password | docker login --username AWS --password-stdin ${ECR}

docker pull ${ECR}/ubuntu:18.04
docker tag ${ECR}/ubuntu:18.04 ubuntu:18.04
docker pull ${ECR}/python:3.9-slim
docker tag ${ECR}/python:3.9-slim python:3.9-slim

docker build -t ${IMAGE} \
   --build-arg FDB_WEBSITE=file:///mnt/website \
   --build-arg FDB_VERSION=$FDB_VERSION \
   --build-arg FDB_ADDITIONAL_VERSIONS=$FDB_VERSION \
   -f release/Dockerfile .

docker tag ${IMAGE} ${ECR}/${IMAGE}

docker build -t ${SIDECAR_IMAGE} \
   --build-arg FDB_WEBSITE=file:///mnt/website \
   --build-arg FDB_VERSION=$FDB_VERSION \
   --build-arg FDB_ADDITIONAL_VERSIONS=$FDB_VERSION \
   -f sidecar/Dockerfile .

docker tag ${SIDECAR_IMAGE} ${ECR}/${SIDECAR_IMAGE}

docker push ${ECR}/${IMAGE}
docker push ${ECR}/${SIDECAR_IMAGE}
