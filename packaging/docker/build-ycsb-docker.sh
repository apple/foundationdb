#!/usr/bin/env bash
set -Eeuo pipefail
set -x

DOCKER_ROOT="$(realpath "$(dirname "${BASH_SOURCE[0]}")")"
BUILD_OUTPUT=$(realpath "${DOCKER_ROOT}"/../..)

echo Docker root:  "${DOCKER_ROOT}"
echo Build output: "${BUILD_OUTPUT}"

cd "${DOCKER_ROOT}"

## eg: CMAKE_PROJECT_VERSION:STATIC=7.0.0
FDB_VERSION=$(grep CMAKE_PROJECT_VERSION: "${BUILD_OUTPUT}"/CMakeCache.txt | cut -d '=' -f 2)

# Options (passed via environment variables)

# Feel free to customize the image tag.
# TODO: add a mechanism to set TAG=FDB_VERSION when we're building public releases.
TAG=${TAG:-${FDB_VERSION}-${OKTETO_NAME}}
ECR=${ECR:-112664522426.dkr.ecr.us-west-2.amazonaws.com}

echo Building with tag "${TAG}"

# Login to ECR
# TODO: Move this to a common place instead of repeatedly copy-pasting it.
aws ecr get-login-password | docker login --username AWS --password-stdin "${ECR}"

docker pull "${ECR}"/openjdk:17-slim
docker tag "${ECR}"/openjdk:17-slim openjdk:17-slim



# derived variables
IMAGE=foundationdb/ycsb:"${TAG}"

# mvn install fdb-java, compile YCSB
mvn install:install-file \
  -Dfile="${BUILD_OUTPUT}"/packages/fdb-java-"${FDB_VERSION}"-PRERELEASE.jar \
  -DgroupId=org.foundationdb \
  -DartifactId=fdb-java \
  -Dversion="${FDB_VERSION}"-PRERELEASE \
  -Dpackaging=jar \
  -DgeneratePom=true
mkdir "${DOCKER_ROOT}"/YCSB && cd "${DOCKER_ROOT}"/YCSB
git clone https://github.com/FoundationDB/YCSB.git .
sed -i "s/<foundationdb.version>[0-9]\+.[0-9]\+.[0-9]\+<\/foundationdb.version>/<foundationdb.version>${FDB_VERSION}-PRERELEASE<\/foundationdb.version>/g" pom.xml
mvn -pl site.ycsb:foundationdb-binding -am clean package
mkdir -p core/target/dependency
# shellcheck disable=SC2046
cp $(find ~/.m2/ -name jax\*.jar) core/target/dependency/
# shellcheck disable=SC2046
cp $(find ~/.m2/ -name htrace\*.jar) core/target/dependency/
# shellcheck disable=SC2046
cp $(find ~/.m2/ -name HdrHistogram\*.jar) core/target/dependency/
rm -rf .git && cd ..

docker build -t "${IMAGE}" -f ycsb/Dockerfile .


docker tag "${IMAGE}" "${ECR}"/"${IMAGE}"


docker push "${ECR}"/"${IMAGE}"
