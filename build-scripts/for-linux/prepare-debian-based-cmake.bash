#!/bin/bash

# install cmake of a version not less than the minimal required
# $1 - minimal required version

set -e

REQUIRED_VERSION=${1:-3.13}

CMAKE_URL=https://github.com/Kitware/CMake/releases/download/v3.26.0/cmake-3.26.0-linux-x86_64.tar.gz

EXISTING_VERSION=`cmake --version | awk '/version/ { print $3; }'` || true

if dpkg --compare-versions "$EXISTING_VERSION" lt "$REQUIRED_VERSION"; then
  echo "Existing cmake $EXISTING_VERSION is absent or too old."

  sudo apt update
  AVAILABLE_APT_VERSION=`apt-cache policy cmake | awk '/Candidate:/{print $2;}'`

  if dpkg --compare-versions "$AVAILABLE_APT_VERSION" lt "$REQUIRED_VERSION"; then
    echo "Available cmake $AVAILABLE_APT_VERSION is too old. Installing one from $CMAKE_URL."
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y wget
    TMP_DIR=`mktemp -d`
    wget -O $TMP_DIR/cmake.tar.gz $CMAKE_URL
    sudo mkdir -p /opt
    sudo tar -xvf $TMP_DIR/cmake.tar.gz -C /opt
    rm -rf $TMP_DIR
    CMAKE_DIR=`ls -d1 /opt/cmake-* | tail -n 1`
    for F in $CMAKE_DIR/bin/*; do
      sudo rm -rf /usr/local/bin/`basename $F`
      sudo ln -s $F /usr/local/bin/
    done
    NEW_VERSION=`cmake --version | awk '/version/ { print $3; }'`
  else
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y cmake
  fi
  NEW_VERSION=`cmake --version | awk '/version/ { print $3; }'`
  echo "Now cmake $NEW_VERSION has been installed."
else
  echo "Existing cmake $EXISTING_VERSION satisfies the minimum version $REQUIRED_VERSION."
fi
