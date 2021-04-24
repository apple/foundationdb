# Run using . build-release-docker.sh

## This is designed to be run inside an okteto environment.

cmk

cd ~/src/foundationdb/

FDB_VERSION=$(grep '  VERSION ' CMakeLists.txt | tr -s ' ' ' ' | cut -d ' ' -f 3)

# Feel free to customize the image tag:
TAG=${TAG:-${FDB_VERSION}-${OKTETO_NAME}}

export IMAGE=foundationdb/foundationdb:${TAG}

echo Building with tag ${TAG}

WEBSITE_BIN_DIR=website/downloads/$FDB_VERSION/linux/
TARBALL=${WEBSITE_BIN_DIR}/fdb_$FDB_VERSION.tar.gz
ECR=112664522426.dkr.ecr.us-west-2.amazonaws.com

cd ~/src/foundationdb/packaging/docker

mkdir -p ${WEBSITE_BIN_DIR}
tar -C ~/build_output/packages/ -zcvf ${TARBALL} bin lib

# XXX
make -C ~/src/fdb-kubernetes-tests/tests/ ecr-login

yes| cp ~/build_output/packages/lib/libfdb_c.so ${WEBSITE_BIN_DIR}/libfdb_c_${FDB_VERSION}.so
docker pull ${ECR}/ubuntu:18.04
docker tag ${ECR}/ubuntu:18.04 ubuntu:18.04

docker build -t ${IMAGE} \
   --build-arg FDB_WEBSITE=file:///mnt/website \
   --build-arg FDB_VERSION=$FDB_VERSION \
   --build-arg FDB_ADDITIONAL_VERSIONS=$FDB_VERSION \
   -f release/Dockerfile .

docker tag ${IMAGE} ${ECR}/${IMAGE}
docker push ${ECR}/${IMAGE}

cd ~/src/fdb-kubernetes-operator/foundationdb-kubernetes-sidecar
echo
pwd
echo

mkdir -p ${WEBSITE_BIN_DIR}
tar -C ~/build_output/packages/ -zcvf ${TARBALL} bin lib
yes| cp ~/build_output/packages/lib/libfdb_c.so ${WEBSITE_BIN_DIR}/libfdb_c_${FDB_VERSION}.so

SIDECAR_IMAGE=foundationdb/foundationdb-kubernetes-sidecar:${TAG}-1

docker pull ${ECR}/python:3.9-slim
docker tag ${ECR}/python:3.9-slim python:3.9-slim

docker build -t ${SIDECAR_IMAGE} \
   --build-arg FDB_WEBSITE=file:///mnt/website \
   --build-arg FDB_VERSION=$FDB_VERSION \
   --build-arg FDB_LIBRARY_VERSIONS=$FDB_VERSION \
   -f Dockerfile .

docker tag ${IMAGE} ${ECR}/${SIDECAR_IMAGE}
docker push ${ECR}/${SIDECAR_IMAGE}

#docker build -f release/Dockerfile -t foundationdb/foundationdb:6.2.29 . --build-arg FDB_VERSION=6.2.29
