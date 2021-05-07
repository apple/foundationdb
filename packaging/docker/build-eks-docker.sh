#!/bin/sh

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

docker pull ${ECR}/amazonlinux:2.0.20210326.0
docker tag ${ECR}/amazonlinux:2.0.20210326.0 amazonlinux:2.0.20210326.0



#derived variables
IMAGE=foundationdb/foundationdb:${TAG}
SIDECAR=foundationdb/foundationdb-kubernetes-sidecar:${TAG}-1
STRIPPED=${STRIPPED:-false}





if $STRIPPED; then
  rsync -av --delete --exclude=*.xml ${BUILD_OUTPUT}/packages/bin .
  rsync -av --delete --exclude=*.a --exclude=*.xml ${BUILD_OUTPUT}/packages/lib .
else
  rsync -av --delete --exclude=*.xml ${BUILD_OUTPUT}/bin .
  rsync -av --delete --exclude=*.a --exclude=*.xml ${BUILD_OUTPUT}/lib .
fi

BUILD_ARGS="--build-arg FDB_VERSION=$FDB_VERSION"

docker build ${BUILD_ARGS} -t ${IMAGE}   --target foundationdb -f Dockerfile.eks .
docker build ${BUILD_ARGS} -t ${SIDECAR} --target sidecar      -f Dockerfile.eks .

docker tag ${IMAGE} ${ECR}/${IMAGE}
docker tag ${SIDECAR} ${ECR}/${SIDECAR}

docker push ${ECR}/${IMAGE}
docker push ${ECR}/${SIDECAR}
