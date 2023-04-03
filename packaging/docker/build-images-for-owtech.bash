#!/bin/bash

# make owtech foundationdb docker images
# $1 - full fdb version, ex. 7.1.25-5.ow.1
# $2 - path to the local distr directory or an url for download
#   (must start with http:// or https://).
#   Default: https://github.com/owtech/foundationdb/releases/download
# $3 - docker registry to push to. If not specified then not to push anywhere
#   Ex. ghcr.io/owtech
#   Podman must be logged in the registry before running this script

set -e

BASE_DIR=`dirname $0`

FDB_VERSION="$1"
DISTR_SRC=${2:-"https://github.com/owtech/foundationdb/releases/download"}
PUSH_REGISTRY="$3"

if [[ -z "$FDB_VERSION" ]]; then
  echo >&2 "Error: FdbVersion is not specified"
  echo >&2 "usage:"
  echo >&2 "  $0 FdbVersion [[FoundationdbDistrDir] PushRegistry]"
  return 1 2>/dev/null || exit 1
fi

TMP_DISTR_DIR=
if [[ "$DISTR_SRC" =~ ^http:|^https: ]]; then
  # download the distr
  TMP_DISTR_DIR=`mktemp -d`
  wget -P "$TMP_DISTR_DIR" \
    "$DISTR_SRC/$FDB_VERSION/foundationdb-bins-$FDB_VERSION.x86_64.tgz" \
    "$DISTR_SRC/$FDB_VERSION/foundationdb-libs-$FDB_VERSION.x86_64.tgz"
  DISTR_DIR="$TMP_DISTR_DIR"
else
  # use the local directory
  DISTR_DIR="$DISTR_SRC"
fi

IMAGE_LIST="foundationdb-base foundationdb foundationdb-kubernetes-sidecar"

# Build and tag images
for IMG in $IMAGE_LIST; do
  echo "Building image $IMG:$FDB_VERSION"
  podman build \
    -f $BASE_DIR/foundationdb-owtech.Dockerfile \
    --build-arg FDB_VERSION=$FDB_VERSION \
    -v `readlink -f $DISTR_DIR`:/mnt/distr:ro,Z \
    --target $IMG \
    -t $IMG:$FDB_VERSION
done

# clear temporary dir
[[ -n "$TMP_DISTR_DIR" ]] && rm -rf "$TMP_DISTR_DIR"

# Push images
if [[ -n "$PUSH_REGISTRY" ]]; then
  for IMG in $IMAGE_LIST; do
    echo "Pushing to $PUSH_REGISTRY/$IMG:$FDB_VERSION"
    podman push $IMG:$FDB_VERSION "$PUSH_REGISTRY/$IMG:$FDB_VERSION"
  done
fi
