#!/bin/sh

set -e

BASE_DIR=`dirname $0`

sudo dnf install -y \
  clang \
  cmake \
  dpkg \
  git \
  java-1.8.0-openjdk-devel \
  mono-core \
  ninja-build \
  python3-sphinx \
  python3-sphinx-bootstrap-theme \
  rpm-build

sudo dnf install -y \
  boost-static \
  bzip2-static \
  jemalloc-devel \
  libatomic \
  libstdc++-static \
  libzstd-static \
  lz4-devel \
  lz4-static \
  xz-static \
  zlib-static

$BASE_DIR/prepare-fedora-openssl.sh

