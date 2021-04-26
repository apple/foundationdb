#!/bin/bash
set -euxo pipefail

## This is designed to be run inside an environment with the following repos checked out under ~/src:
#
#     foundationdb
#     fdb-kubernetes-operator
#
# The foundationdb build will write its output to ~/build_output

FDB_VERSION=$(grep '  VERSION ' ~/src/foundationdb/CMakeLists.txt | tr -s ' ' ' ' | cut -d ' ' -f 3)

# Options (passed via environment variables)

# Feel free to customize the image tag.
# TODO: add a mechanism to set TAG=FDB_VERSION when we're building public releases.
TAG=${TAG:-${FDB_VERSION}-${OKTETO_NAME}}
ECR=${ECR:-112664522426.dkr.ecr.us-west-2.amazonaws.com}

echo Building with tag ${TAG}

# TODO: This is a copy of the commonly-used 'cmk' function.
cmake -S ${HOME}/src/foundationdb -B ${HOME}/build_output \
   -D USE_CCACHE=ON -D USE_WERROR=ON -D RocksDB_ROOT=/opt/rocksdb-6.10.1 -D RUN_JUNIT_TESTS=ON -D RUN_JAVA_INTEGRATION_TESTS=ON \
   -G Ninja \
      && ninja -C ${HOME}/build_output -j 84


# derived variables
IMAGE=foundationdb/foundationdb:${TAG}
SIDECAR_IMAGE=foundationdb/foundationdb-kubernetes-sidecar:${TAG}-1

WEBSITE_BIN_DIR=website/downloads/${FDB_VERSION}/linux/
TARBALL=${WEBSITE_BIN_DIR}/fdb_${FDB_VERSION}.tar.gz

# copy packaging scripts from operator repo into fdb build_output directory
cp -an ~/src/fdb-kubernetes-operator/foundationdb-kubernetes-sidecar/* ~/build_output/packages/docker/

cd ~/build_output/packages/docker

mkdir -p ${WEBSITE_BIN_DIR}
tar -C ~/build_output/packages/ -zcvf ${TARBALL} bin lib
cp ~/build_output/packages/lib/libfdb_c.so ${WEBSITE_BIN_DIR}/libfdb_c_${FDB_VERSION}.so

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
   --build-arg FDB_LIBRARY_VERSIONS=$FDB_VERSION \
   -f Dockerfile .

docker tag ${SIDECAR_IMAGE} ${ECR}/${SIDECAR_IMAGE}

# Login to ECR
# TODO: Move this to a common place instead of repeatedly copy-pasting it.
aws ecr get-login-password | docker login --username AWS --password-stdin ${ECR}

docker push ${ECR}/${IMAGE}
docker push ${ECR}/${SIDECAR_IMAGE}
