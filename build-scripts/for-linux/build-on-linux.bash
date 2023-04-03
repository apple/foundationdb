#!/bin/bash

# $1 - Version
# $2 - Build version
# $3 - Release flag
# $4 - Paralllel threads
# $5 - Source Dir. If not set then relative to the script dir

set -e

BASE_DIR="$(readlink -f $(dirname $0))"
source $BASE_DIR/../set-ver-prms.sh "$1" "$2"
RELEASE_FLAG=${3:-OFF}
PARALLEL_PRMS="-j ${4:-$(nproc)}"
SRC_DIR=${5:-$(readlink -f $BASE_DIR/../..)}

START_DIR=`pwd`
BUILD_DIR=$START_DIR/bld/linux

mkdir -p $BUILD_DIR
pushd $BUILD_DIR

rm -rf *
export LANG=C

APP_PRMS="\
  $CMAKE_VERSION_PRMS \
  -DFDB_RELEASE=$RELEASE_FLAG \
  -DGENERATE_DEBUG_PACKAGES=OFF \
  -DUSE_LIBCXX=OFF \
  -DSTATIC_LINK_LIBCXX=ON \
  -DENABLE_SIMULATION_TESTS=ON"

[ ! -e /usr/lib64/libcrypto.a -a -e /opt/openssl/lib/libcrypto.a ] && \
  APP_PRMS="$APP_PRMS -DOPENSSL_ROOT_DIR=/opt/openssl"

echo "env CC=clang CXX=clang++ cmake -G Ninja $APP_PRMS . $SRC_DIR"
env CC=clang CXX=clang++ cmake -G Ninja $APP_PRMS . $SRC_DIR

ninja $PARALLEL_PRMS -k 0

cpack -G RPM
for fn in packages/*.rpm; do echo "$fn:"; rpm -qpRv $fn; echo; done

cpack -G DEB
for fn in packages/*.deb; do echo "$fn:"; dpkg -I $fn; done

ninja $PARALLEL_PRMS -k 0 documentation/package_html

# make all-binaries and libraries archives
# calculate filenames
# TO DO: calculate the processor architecture instead of hardcoding x86_64
BINS_FILENAME=`ls -1 packages/foundationdb-docs-* | sed s/-docs/-bins/ | sed s/.tgz/.x86_64.tgz/`
LIBS_FILENAME=`ls -1 packages/foundationdb-docs-* | sed s/-docs/-libs/ | sed s/.tgz/.x86_64.tgz/`
tar -I pigz -cvf $BINS_FILENAME -C packages/bin .
tar -I pigz -cvf $LIBS_FILENAME -C packages/lib .

popd

